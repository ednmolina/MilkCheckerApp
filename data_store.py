"""
CSV-only data storage for FairPrice stock tracker.
No SQLite - all data persisted as CSV files in the data/ directory.

Optimizations:
- _read_csv_cached() reads a file once and caches by mtime
- get_latest_store_stock() reads only the latest batch (not all history)
- get_batch_ids() scans only the batch_id column, not full rows
"""

import csv
import os
import json
import math
from datetime import datetime, timezone
from typing import Optional

# If MEIJI_APP_BUNDLE is set (by the .app launcher), save to ~/Documents/
# Otherwise save to the project's local data/ folder
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

if os.environ.get("MEIJI_APP_BUNDLE"):
    DATA_DIR = os.path.join(os.path.expanduser("~"), "Documents", "Meiji Milk Tracker Data")
else:
    DATA_DIR = os.path.join(_SCRIPT_DIR, "data")
STORES_JSON = os.path.join(_SCRIPT_DIR, "stores_with_coords.json")

# CSV file paths
STORE_STOCK_CSV = os.path.join(DATA_DIR, "store_stock_history.csv")
WAREHOUSE_CSV = os.path.join(DATA_DIR, "warehouse_history.csv")
STORES_CSV = os.path.join(DATA_DIR, "stores.csv")

STORE_STOCK_FIELDS = [
    "batch_id", "timestamp", "store_id", "store_name", "store_type", "address",
    "lat", "lng", "in_store_stock", "sap_stock", "price", "mrp"
]

WAREHOUSE_FIELDS = [
    "timestamp", "in_store_stock", "online_stock", "sap_stock",
    "price", "mrp", "discount", "product_name"
]

STORES_FIELDS = [
    "id", "name", "address", "lat", "lng", "postal_code", "store_type", "zone_id"
]

# --- In-memory cache keyed by (filepath, mtime) ---
_csv_cache: dict[str, tuple[float, list[dict]]] = {}


def _read_csv_cached(path: str) -> list[dict]:
    """
    Read a CSV file and cache the result. Returns cached data if the file
    hasn't been modified since last read. Avoids re-reading the same file
    multiple times per render cycle.
    """
    if not os.path.exists(path):
        return []

    mtime = os.path.getmtime(path)
    cached = _csv_cache.get(path)
    if cached and cached[0] == mtime:
        return cached[1]

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))

    _csv_cache[path] = (mtime, rows)
    return rows


def invalidate_cache():
    """Clear the in-memory CSV cache (call after writes)."""
    _csv_cache.clear()


# --- Directory & init -----------------------------------------

def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _init_csv(path, fields):
    """Create CSV with header if it doesn't exist."""
    if not os.path.exists(path):
        ensure_data_dir()
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()


def generate_batch_id():
    """Generate a batch ID for grouping stores checked in the same run."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# --- Stores ---------------------------------------------------

def load_stores():
    """Load all stores from the JSON file."""
    if os.path.exists(STORES_JSON):
        with open(STORES_JSON) as f:
            return json.load(f)
    return []


def save_stores_csv(stores):
    """Save stores list to CSV."""
    ensure_data_dir()
    with open(STORES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STORES_FIELDS)
        writer.writeheader()
        for s in stores:
            writer.writerow({
                "id": s.get("id", ""),
                "name": s.get("name", ""),
                "address": s.get("address", ""),
                "lat": s.get("lat", ""),
                "lng": s.get("lng", ""),
                "postal_code": s.get("postalCode", ""),
                "store_type": s.get("storeType", ""),
                "zone_id": s.get("zoneId", ""),
            })
    invalidate_cache()


# --- Warehouse ------------------------------------------------

def append_warehouse_snapshot(data):
    """Append a warehouse stock snapshot."""
    _init_csv(WAREHOUSE_CSV, WAREHOUSE_FIELDS)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(WAREHOUSE_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=WAREHOUSE_FIELDS)
        writer.writerow({
            "timestamp": now,
            "in_store_stock": data.get("in_store_stock", 0),
            "online_stock": data.get("online_stock", 0),
            "sap_stock": data.get("sap_stock", 0),
            "price": data.get("price", 0),
            "mrp": data.get("mrp", 0),
            "discount": data.get("discount", 0),
            "product_name": data.get("product_name", ""),
        })
    invalidate_cache()


def read_warehouse_history():
    """Read all warehouse history from CSV (cached by mtime)."""
    rows = _read_csv_cached(WAREHOUSE_CSV)
    parsed = []
    for r in rows:
        row = dict(r)
        for k in ["in_store_stock", "online_stock", "sap_stock"]:
            row[k] = int(float(row.get(k, 0) or 0))
        for k in ["price", "mrp", "discount"]:
            row[k] = float(row.get(k, 0) or 0)
        parsed.append(row)
    return parsed


def get_latest_warehouse():
    """Get the most recent warehouse snapshot."""
    history = read_warehouse_history()
    if history:
        return history[-1]
    return None


# --- Store stock ----------------------------------------------

def append_store_stock(batch_id, store_id, store_name, store_type, address, lat, lng,
                       in_store_stock, sap_stock, price, mrp):
    """Append a per-store stock snapshot with batch_id for grouping."""
    _init_csv(STORE_STOCK_CSV, STORE_STOCK_FIELDS)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open(STORE_STOCK_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STORE_STOCK_FIELDS)
        writer.writerow({
            "batch_id": batch_id,
            "timestamp": now,
            "store_id": store_id,
            "store_name": store_name,
            "store_type": store_type,
            "address": address,
            "lat": lat,
            "lng": lng,
            "in_store_stock": in_store_stock,
            "sap_stock": sap_stock,
            "price": price,
            "mrp": mrp,
        })
    invalidate_cache()


def _parse_store_stock_rows(rows: list[dict]) -> list[dict]:
    """Parse numeric fields in store stock rows."""
    parsed = []
    for r in rows:
        row = dict(r)
        row["in_store_stock"] = int(float(row.get("in_store_stock", 0) or 0))
        row["sap_stock"] = int(float(row.get("sap_stock", 0) or 0))
        row["price"] = float(row.get("price", 0) or 0)
        row["mrp"] = float(row.get("mrp", 0) or 0)
        row["lat"] = float(row.get("lat", 0) or 0)
        row["lng"] = float(row.get("lng", 0) or 0)
        parsed.append(row)
    return parsed


def read_store_stock_history():
    """Read all store stock history from CSV (cached by mtime)."""
    rows = _read_csv_cached(STORE_STOCK_CSV)
    return _parse_store_stock_rows(rows)


def get_latest_store_stock():
    """
    Get the most recent stock snapshot for each store.
    Optimized: scans from the end of the file to find the latest batch,
    then only parses rows from that batch.
    """
    rows = _read_csv_cached(STORE_STOCK_CSV)
    if not rows:
        return {}

    # Find the latest batch_id by scanning from the end
    latest_batch = None
    for r in reversed(rows):
        bid = r.get("batch_id", "")
        if bid:
            latest_batch = bid
            break

    if not latest_batch:
        return {}

    # Only parse rows from the latest batch
    latest = {}
    for r in rows:
        if r.get("batch_id") == latest_batch:
            parsed = dict(r)
            parsed["in_store_stock"] = int(float(parsed.get("in_store_stock", 0) or 0))
            parsed["sap_stock"] = int(float(parsed.get("sap_stock", 0) or 0))
            parsed["price"] = float(parsed.get("price", 0) or 0)
            parsed["mrp"] = float(parsed.get("mrp", 0) or 0)
            parsed["lat"] = float(parsed.get("lat", 0) or 0)
            parsed["lng"] = float(parsed.get("lng", 0) or 0)
            latest[str(parsed["store_id"])] = parsed

    return latest


def get_batch_ids():
    """
    Get all unique batch IDs sorted by time (newest first).
    Optimized: only reads the batch_id column.
    """
    rows = _read_csv_cached(STORE_STOCK_CSV)
    batches = sorted(
        set(r.get("batch_id", "") for r in rows if r.get("batch_id")),
        reverse=True,
    )
    return batches


def get_batch_data(batch_id):
    """Get all store stock data for a specific batch."""
    rows = _read_csv_cached(STORE_STOCK_CSV)
    batch_rows = [r for r in rows if r.get("batch_id") == batch_id]
    return _parse_store_stock_rows(batch_rows)


# --- Geo helpers ----------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
