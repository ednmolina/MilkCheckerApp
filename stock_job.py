"""
Stock checking job - fetches real per-store stock from FairPrice API.

Optimizations:
- Smart delta checking: skips stores whose stock was 0 or -1 in the last 3 checks
  (unlikely to restock frequently). Checks them every 6th run instead.
- Stores CSV written only on first run or when stores.json changes.
- Warehouse price fetched once per hour (cached in fairprice_api).

API calls per run:
- Full run (first or every 6th): ~202 store calls + 1 warehouse + 1 price = ~204
- Smart run (normal hourly): ~80-120 store calls + 1 warehouse = ~81-121
"""

import sys
import os
import time
from typing import Callable, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fairprice_api import PRODUCTS, PRODUCT_SKU, get_warehouse_stock, get_store_stock
from data_store import (
    load_stores, save_stores_csv, append_warehouse_snapshot,
    append_store_stock_batch, ensure_data_dir, generate_batch_id,
    get_latest_store_stock, get_batch_ids, using_gsheets_backend, STORES_CSV,
)


def _emit_progress(progress_callback: Optional[Callable[[dict], None]], **payload) -> None:
    """Send progress events to the caller when a callback is provided."""
    if progress_callback is None:
        return
    progress_callback(payload)

def _should_check_store(store_id: str, latest_stock: dict, run_count: int) -> bool:
    """
    Decide whether to check a store this run.
    - Always check on full runs (every 6th run, i.e. run_count % 6 == 0)
    - Always check stores that had stock > 0 last time (stock changes matter)
    - Always check stores never checked before
    - Skip stores that were -1 (not available) for the last 3+ runs - check every 6th run
    - Skip stores at 0 stock - check every 3rd run (might restock)
    """
    if run_count % 6 == 0:
        return True  # Full check every 6 hours

    stock_data = latest_stock.get(str(store_id))
    if not stock_data:
        return True  # Never checked - must check

    last_stock = stock_data.get("in_store_stock", -1)

    if last_stock > 0:
        return True  # Has stock - always check (stock could drop)
    if last_stock == 0:
        return run_count % 3 == 0  # Out of stock - check every 3rd run
    # last_stock < 0 (not available at this store)
    return False  # Skip - product not carried here, check on full runs only

def _get_run_count(product_sku: str = PRODUCT_SKU) -> int:
    """
    Track how many historical batches already exist.
    Returns the current run number (0-indexed).
    """
    return len(get_batch_ids(product_sku=product_sku))

def _stores_csv_needs_update() -> bool:
    """Check if the stores export needs to be refreshed."""
    if using_gsheets_backend():
        return True
    if not os.path.exists(STORES_CSV):
        return True
    stores_json = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stores_with_coords.json")
    if not os.path.exists(stores_json):
        return False
    return os.path.getmtime(stores_json) > os.path.getmtime(STORES_CSV)

def run_stock_check(verbose=True, progress_callback: Optional[Callable[[dict], None]] = None):
    """
    Run a stock check cycle with smart delta checking.
    - Full run every 6th execution (checks all stores)
    - Smart runs skip stores unlikely to have changed
    """
    ensure_data_dir()

    batch_id = generate_batch_id()
    if verbose:
        print(f"Batch ID: {batch_id}")

    # Load all stores
    _emit_progress(
        progress_callback,
        stage="preparing",
        fraction=0.0,
        text="Loading store list...",
    )
    stores = load_stores()
    if not stores:
        if verbose:
            print("No stores found in stores_with_coords.json")
        return {"success": False, "message": "No stores found"}

    valid_stores = [s for s in stores if s.get("lat") and s.get("lng")]
    all_rows = []
    product_messages = []
    stores_csv_needs_update = _stores_csv_needs_update()

    total_products = len(PRODUCTS)
    product_plans = []
    for product_key, product in PRODUCTS.items():
        product_name = product["name"]
        product_sku = product["sku"]
        run_count = _get_run_count(product_sku=product_sku)
        is_full_run = (run_count % 6 == 0)
        latest_stock = get_latest_store_stock(product_sku=product_sku)

        stores_to_check = []
        stores_to_skip = []
        for store in valid_stores:
            sid = str(store["id"])
            if _should_check_store(sid, latest_stock, run_count):
                stores_to_check.append(store)
            else:
                stores_to_skip.append(store)

        product_plans.append({
            "product_key": product_key,
            "product_name": product_name,
            "product_sku": product_sku,
            "run_count": run_count,
            "is_full_run": is_full_run,
            "latest_stock": latest_stock,
            "stores_to_check": stores_to_check,
            "stores_to_skip": stores_to_skip,
        })

    total_store_checks = sum(len(plan["stores_to_check"]) for plan in product_plans)
    total_work_units = 1 + total_products + total_store_checks + 1
    if stores_csv_needs_update:
        total_work_units += 1
    completed_work_units = 1

    _emit_progress(
        progress_callback,
        stage="preparing",
        fraction=completed_work_units / total_work_units,
        text=(
            f"Prepared stock check plan for {total_products} products "
            f"across {total_store_checks} store requests."
        ),
        total_products=total_products,
        total_store_checks=total_store_checks,
    )

    # Only rewrite stores.csv if needed
    if stores_csv_needs_update:
        _emit_progress(
            progress_callback,
            stage="syncing_store_list",
            fraction=completed_work_units / total_work_units,
            text="Syncing store list to the active backend...",
        )
        save_stores_csv(stores)
        completed_work_units += 1
        if verbose:
            print("Updated stores.csv")
        _emit_progress(
            progress_callback,
            stage="syncing_store_list",
            fraction=completed_work_units / total_work_units,
            text="Store list synced. Starting stock checks...",
        )

    for p_idx, plan in enumerate(product_plans):
        product_key = plan["product_key"]
        product_name = plan["product_name"]
        product_sku = plan["product_sku"]
        run_count = plan["run_count"]
        is_full_run = plan["is_full_run"]
        latest_stock = plan["latest_stock"]
        stores_to_check = plan["stores_to_check"]
        stores_to_skip = plan["stores_to_skip"]

        if verbose:
            run_type = "FULL" if is_full_run else "SMART"
            print(f"\n[{product_key}] {product_name}")
            print(f"Run #{run_count} ({run_type} check)")
            print("Checking warehouse stock...")

        _emit_progress(
            progress_callback,
            stage="warehouse",
            fraction=completed_work_units / total_work_units,
            text=f"[{p_idx + 1}/{total_products}] Checking warehouse for {product_name}...",
            product_key=product_key,
            product_name=product_name,
            product_index=p_idx + 1,
            total_products=total_products,
        )
        warehouse = get_warehouse_stock(product_sku=product_sku)
        if warehouse:
            append_warehouse_snapshot(warehouse)
            if verbose:
                print(
                    f"  Warehouse: {warehouse['in_store_stock']} units, "
                    f"${warehouse['price']} (MRP ${warehouse['mrp']})"
                )
        else:
            if verbose:
                print("  WARNING: Could not fetch warehouse stock")
        completed_work_units += 1

        if verbose:
            print(
                f"Checking {len(stores_to_check)} stores "
                f"(skipping {len(stores_to_skip)} unchanged stores)..."
            )

        _emit_progress(
            progress_callback,
            stage="stores",
            fraction=completed_work_units / total_work_units,
            text=(
                f"[{p_idx + 1}/{total_products}] {product_name}: "
                f"checking 0/{len(stores_to_check)} stores "
                f"(skipping {len(stores_to_skip)} unchanged)."
            ),
            product_key=product_key,
            product_name=product_name,
            product_index=p_idx + 1,
            total_products=total_products,
            checked=0,
            total=len(stores_to_check),
            skipped=len(stores_to_skip),
        )

        checked = 0
        in_stock = 0
        out_of_stock = 0
        not_found = 0
        
        for store in stores_to_check:
            store_id = store["id"]
            store_name = store.get("name", f"Store {store_id}")

            result = get_store_stock(str(store_id), product_sku=product_sku)

            if result and result["in_store_stock"] >= 0:
                all_rows.append({
                    "batch_id": batch_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "store_type": store.get("storeType", ""),
                    "address": store.get("address", ""),
                    "lat": store.get("lat", 0),
                    "lng": store.get("lng", 0),
                    "in_store_stock": result["in_store_stock"],
                    "sap_stock": result["sap_stock"],
                    "price": result["price"],
                    "mrp": result["mrp"],
                    "product_sku": product_sku,
                })
                if result["in_store_stock"] > 0:
                    in_stock += 1
                else:
                    out_of_stock += 1
            else:
                all_rows.append({
                    "batch_id": batch_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "store_type": store.get("storeType", ""),
                    "address": store.get("address", ""),
                    "lat": store.get("lat", 0),
                    "lng": store.get("lng", 0),
                    "in_store_stock": -1,
                    "sap_stock": -1,
                    "price": 0,
                    "mrp": 0,
                    "product_sku": product_sku,
                })
                not_found += 1

            checked += 1
            completed_work_units += 1
            if verbose and checked % 20 == 0:
                print(f"  Checked {checked}/{len(stores_to_check)} stores...")
            _emit_progress(
                progress_callback,
                stage="stores",
                fraction=completed_work_units / total_work_units,
                text=(
                    f"[{p_idx + 1}/{total_products}] {product_name}: "
                    f"checked {checked}/{len(stores_to_check)} stores "
                    f"(skipping {len(stores_to_skip)} unchanged)."
                ),
                product_key=product_key,
                product_name=product_name,
                product_index=p_idx + 1,
                total_products=total_products,
                checked=checked,
                total=len(stores_to_check),
                skipped=len(stores_to_skip),
                current_store_id=store_id,
                current_store_name=store_name,
            )

            time.sleep(0.15)

        for store in stores_to_skip:
            sid = str(store["id"])
            prev = latest_stock.get(sid, {})
            all_rows.append({
                "batch_id": batch_id,
                "store_id": store["id"],
                "store_name": store.get("name", f"Store {sid}"),
                "store_type": store.get("storeType", ""),
                "address": store.get("address", ""),
                "lat": store.get("lat", 0),
                "lng": store.get("lng", 0),
                "in_store_stock": prev.get("in_store_stock", -1),
                "sap_stock": prev.get("sap_stock", -1),
                "price": prev.get("price", 0),
                "mrp": prev.get("mrp", 0),
                "product_sku": product_sku,
            })

        product_messages.append(
            f"{product_key}: checked {checked}/{len(valid_stores)} stores "
            f"(skipped {len(stores_to_skip)}) - "
            f"{in_stock} in stock, {out_of_stock} out of stock, {not_found} not found"
        )

    _emit_progress(
        progress_callback,
        stage="saving",
        fraction=completed_work_units / total_work_units,
        text=f"Saving {len(all_rows)} store snapshots to the active backend...",
        rows=len(all_rows),
    )
    append_store_stock_batch(all_rows)
    completed_work_units += 1

    _emit_progress(
        progress_callback,
        stage="complete",
        fraction=completed_work_units / total_work_units,
        text="Stock check complete.",
        rows=len(all_rows),
    )

    msg = f"Batch {batch_id}: " + " | ".join(product_messages)
    if verbose:
        print(f"\n{msg}")

    return {"success": True, "message": msg, "batch_id": batch_id}

if __name__ == "__main__":
    print("=" * 60)
    print("FairPrice Stock Check Job")
    print("=" * 60)
    run_stock_check(verbose=True)
    print("\nJob complete!")
