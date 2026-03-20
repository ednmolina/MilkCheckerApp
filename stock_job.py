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
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fairprice_api import get_warehouse_stock, get_store_stock
from data_store import (
    load_stores, save_stores_csv, append_warehouse_snapshot,
    append_store_stock, ensure_data_dir, generate_batch_id,
    get_latest_store_stock, STORES_CSV,
)

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

def _get_run_count() -> int:
    """
    Track how many times the job has run using a simple counter file.
    Returns the current run number (0-indexed).
    """
    counter_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", ".run_count")
    count = 0
    if os.path.exists(counter_file):
        try:
            with open(counter_file) as f:
                count = int(f.read().strip())
        except (ValueError, IOError):
            count = 0

    # Increment and save
    with open(counter_file, "w") as f:
        f.write(str(count + 1))

    return count

def _stores_csv_needs_update() -> bool:
    """Check if stores.csv needs to be written (missing or older than stores.json)."""
    if not os.path.exists(STORES_CSV):
        return True
    stores_json = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stores_with_coords.json")
    if not os.path.exists(stores_json):
        return False
    return os.path.getmtime(stores_json) > os.path.getmtime(STORES_CSV)

def run_stock_check(verbose=True):
    """
    Run a stock check cycle with smart delta checking.
    - Full run every 6th execution (checks all stores)
    - Smart runs skip stores unlikely to have changed
    """
    ensure_data_dir()

    batch_id = generate_batch_id()
    run_count = _get_run_count()
    is_full_run = (run_count % 6 == 0)

    if verbose:
        run_type = "FULL" if is_full_run else "SMART"
        print(f"Batch ID: {batch_id} (Run #{run_count}, {run_type} check)")

    # Load all stores
    stores = load_stores()
    if not stores:
        if verbose:
            print("No stores found in stores_with_coords.json")
        return {"success": False, "message": "No stores found"}

    # Only rewrite stores.csv if needed
    if _stores_csv_needs_update():
        save_stores_csv(stores)
        if verbose:
            print("Updated stores.csv")

    # 1. Check warehouse stock (1 API call + maybe 1 for price if cache expired)
    if verbose:
        print("Checking warehouse stock...")
    warehouse = get_warehouse_stock()
    if warehouse:
        append_warehouse_snapshot(warehouse)
        if verbose:
            print(f"  Warehouse: {warehouse['in_store_stock']} units, "
                  f"${warehouse['price']} (MRP ${warehouse['mrp']})")
    else:
        if verbose:
            print("  WARNING: Could not fetch warehouse stock")

    # 2. Smart per-store stock check
    valid_stores = [s for s in stores if s.get("lat") and s.get("lng")]
    latest_stock = get_latest_store_stock()

    # Decide which stores to check this run
    stores_to_check = []
    stores_to_skip = []
    for store in valid_stores:
        sid = str(store["id"])
        if _should_check_store(sid, latest_stock, run_count):
            stores_to_check.append(store)
        else:
            stores_to_skip.append(store)

    if verbose:
        print(f"\nChecking {len(stores_to_check)} stores "
              f"(skipping {len(stores_to_skip)} unchanged stores)...")

    checked = 0
    in_stock = 0
    out_of_stock = 0
    not_found = 0

    for store in stores_to_check:
        store_id = store["id"]
        store_name = store.get("name", f"Store {store_id}")

        result = get_store_stock(str(store_id))

        if result and result["in_store_stock"] >= 0:
            append_store_stock(
                batch_id=batch_id,
                store_id=store_id,
                store_name=store_name,
                store_type=store.get("storeType", ""),
                address=store.get("address", ""),
                lat=store.get("lat", 0),
                lng=store.get("lng", 0),
                in_store_stock=result["in_store_stock"],
                sap_stock=result["sap_stock"],
                price=result["price"],
                mrp=result["mrp"],
            )
            if result["in_store_stock"] > 0:
                in_stock += 1
            else:
                out_of_stock += 1
        else:
            append_store_stock(
                batch_id=batch_id,
                store_id=store_id,
                store_name=store_name,
                store_type=store.get("storeType", ""),
                address=store.get("address", ""),
                lat=store.get("lat", 0),
                lng=store.get("lng", 0),
                in_store_stock=-1,
                sap_stock=-1,
                price=0,
                mrp=0,
            )
            not_found += 1

        checked += 1
        if verbose and checked % 20 == 0:
            print(f"  Checked {checked}/{len(stores_to_check)} stores...")

        # Adaptive delay: 0.15s between calls (slightly faster with connection reuse)
        time.sleep(0.15)

    # For skipped stores, carry forward their last known data into this batch
    for store in stores_to_skip:
        sid = str(store["id"])
        prev = latest_stock.get(sid, {})
        append_store_stock(
            batch_id=batch_id,
            store_id=store["id"],
            store_name=store.get("name", f"Store {sid}"),
            store_type=store.get("storeType", ""),
            address=store.get("address", ""),
            lat=store.get("lat", 0),
            lng=store.get("lng", 0),
            in_store_stock=prev.get("in_store_stock", -1),
            sap_stock=prev.get("sap_stock", -1),
            price=prev.get("price", 0),
            mrp=prev.get("mrp", 0),
        )

    msg = (f"Batch {batch_id}: Checked {checked}/{len(valid_stores)} stores "
           f"(skipped {len(stores_to_skip)}) - "
           f"{in_stock} in stock, {out_of_stock} out of stock, {not_found} not found")
    if verbose:
        print(f"\n{msg}")

    return {"success": True, "message": msg, "batch_id": batch_id}

if __name__ == "__main__":
    print("=" * 60)
    print("FairPrice Stock Check Job")
    print("=" * 60)
    run_stock_check(verbose=True)
    print("\nJob complete!")
