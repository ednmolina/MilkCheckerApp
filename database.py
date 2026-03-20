"""
SQLite database module for FairPrice stock tracker.
Stores stock snapshots, store cache, and historical data.
Also exports data to CSV files for easy access and sharing.
"""

import sqlite3
import os
import csv
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fairprice_stock.db")
CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv_exports")

@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS store_cache (
                fp_store_id TEXT PRIMARY KEY,
                identifier TEXT,
                name TEXT NOT NULL,
                address TEXT,
                store_type TEXT,
                lat REAL,
                lng REAL,
                phone TEXT,
                is_search_browse_enabled INTEGER DEFAULT 0,
                zone_id TEXT,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stock_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id TEXT NOT NULL,
                store_name TEXT,
                stock_level INTEGER,
                stock_buffer INTEGER,
                product_status TEXT,
                price REAL,
                mrp REAL,
                offer_price REAL,
                discount REAL,
                checked_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_store_checked
                ON stock_snapshots(store_id, checked_at);
            CREATE INDEX IF NOT EXISTS idx_snapshots_checked
                ON stock_snapshots(checked_at);
        """)

    # Ensure CSV export directory exists
    os.makedirs(CSV_DIR, exist_ok=True)


# ==================== Store Cache ====================

def upsert_store(store: dict):
    """Insert or update a store in the cache."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO store_cache (fp_store_id, identifier, name, address, store_type,
                                     lat, lng, phone, is_search_browse_enabled, zone_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fp_store_id) DO UPDATE SET
                identifier=excluded.identifier, name=excluded.name, address=excluded.address,
                store_type=excluded.store_type, lat=excluded.lat, lng=excluded.lng,
                phone=excluded.phone, is_search_browse_enabled=excluded.is_search_browse_enabled,
                zone_id=excluded.zone_id, updated_at=excluded.updated_at
        """, (
            store["fp_store_id"], store.get("identifier"), store["name"],
            store.get("address"), store.get("store_type"),
            store.get("lat"), store.get("lng"), store.get("phone"),
            1 if store.get("is_search_browse_enabled") else 0,
            store.get("zone_id"), int(time.time() * 1000)
        ))

def bulk_upsert_stores(stores: list[dict]):
    """Bulk upsert stores into the cache."""
    for store in stores:
        upsert_store(store)

def get_all_cached_stores() -> list[dict]:
    """Get all cached stores."""
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM store_cache ORDER BY name").fetchall()
        return [dict(r) for r in rows]


# ==================== Stock Snapshots ====================

def insert_snapshot(snapshot: dict):
    """Insert a stock snapshot."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO stock_snapshots (store_id, store_name, stock_level, stock_buffer,
                                         product_status, price, mrp, offer_price, discount, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot["store_id"], snapshot.get("store_name"),
            snapshot.get("stock_level"), snapshot.get("stock_buffer"),
            snapshot.get("product_status"), snapshot.get("price"),
            snapshot.get("mrp"), snapshot.get("offer_price"),
            snapshot.get("discount"), snapshot["checked_at"]
        ))

def insert_snapshots(snapshots: list[dict]):
    """Insert multiple stock snapshots."""
    with get_db() as conn:
        conn.executemany("""
            INSERT INTO stock_snapshots (store_id, store_name, stock_level, stock_buffer,
                                         product_status, price, mrp, offer_price, discount, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [(
            s["store_id"], s.get("store_name"),
            s.get("stock_level"), s.get("stock_buffer"),
            s.get("product_status"), s.get("price"),
            s.get("mrp"), s.get("offer_price"),
            s.get("discount"), s["checked_at"]
        ) for s in snapshots])

def get_latest_warehouse_snapshot() -> Optional[dict]:
    """Get the most recent warehouse snapshot."""
    with get_db() as conn:
        row = conn.execute("""
            SELECT * FROM stock_snapshots
            WHERE store_id = 'warehouse'
            ORDER BY checked_at DESC LIMIT 1
        """).fetchone()
        return dict(row) if row else None

def get_warehouse_history(from_time: int, to_time: int, limit: int = 1000) -> list[dict]:
    """Get warehouse stock history within a time range."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM stock_snapshots
            WHERE store_id = 'warehouse' AND checked_at >= ? AND checked_at <= ?
            ORDER BY checked_at ASC LIMIT ?
        """, (from_time, to_time, limit)).fetchall()
        return [dict(r) for r in rows]

def get_store_history(store_id: str, from_time: int, to_time: int, limit: int = 500) -> list[dict]:
    """Get stock history for a specific store."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM stock_snapshots
            WHERE store_id = ? AND checked_at >= ? AND checked_at <= ?
            ORDER BY checked_at ASC LIMIT ?
        """, (store_id, from_time, to_time, limit)).fetchall()
        return [dict(r) for r in rows]

def get_all_latest_snapshots() -> list[dict]:
    """Get the most recent snapshot for each store."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT s.* FROM stock_snapshots s
            INNER JOIN (
                SELECT store_id, MAX(checked_at) as max_checked
                FROM stock_snapshots
                GROUP BY store_id
            ) latest ON s.store_id = latest.store_id AND s.checked_at = latest.max_checked
        """).fetchall()
        return [dict(r) for r in rows]

def get_snapshot_count() -> int:
    """Get total number of snapshots."""
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM stock_snapshots").fetchone()
        return row["cnt"] if row else 0

def get_unique_check_times() -> list[int]:
    """Get all unique check timestamps for warehouse."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT checked_at FROM stock_snapshots
            WHERE store_id = 'warehouse'
            ORDER BY checked_at ASC
        """).fetchall()
        return [r["checked_at"] for r in rows]


# ==================== CSV Export ====================

def _ts_to_str(ts_ms):
    """Convert millisecond timestamp to readable datetime string."""
    if ts_ms is None:
        return ""
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return str(ts_ms)

def export_snapshots_csv() -> str:
    """
    Export all stock snapshots to a CSV file.
    Returns the file path of the exported CSV.
    """
    os.makedirs(CSV_DIR, exist_ok=True)
    filepath = os.path.join(CSV_DIR, "stock_snapshots.csv")

    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM stock_snapshots ORDER BY checked_at DESC
        """).fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "store_id", "store_name", "stock_level", "stock_buffer",
            "product_status", "price", "mrp", "offer_price", "discount",
            "checked_at_ms", "checked_at_datetime"
        ])
        for row in rows:
            d = dict(row)
            writer.writerow([
                d["id"], d["store_id"], d["store_name"], d["stock_level"],
                d["stock_buffer"], d["product_status"], d["price"], d["mrp"],
                d["offer_price"], d["discount"], d["checked_at"],
                _ts_to_str(d["checked_at"])
            ])

    return filepath

def export_stores_csv() -> str:
    """
    Export all cached stores to a CSV file.
    Returns the file path of the exported CSV.
    """
    os.makedirs(CSV_DIR, exist_ok=True)
    filepath = os.path.join(CSV_DIR, "store_cache.csv")

    with get_db() as conn:
        rows = conn.execute("SELECT * FROM store_cache ORDER BY name").fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "fp_store_id", "identifier", "name", "address", "store_type",
            "lat", "lng", "phone", "is_search_browse_enabled", "zone_id",
            "updated_at_ms", "updated_at_datetime"
        ])
        for row in rows:
            d = dict(row)
            # Skip stores with no name or invalid coordinates
            if not d.get("name") or not d["name"].strip():
                continue
            if d.get("lat", 0) == 0 and d.get("lng", 0) == 0:
                continue
            writer.writerow([
                d["fp_store_id"], d["identifier"], d["name"], d["address"],
                d["store_type"], d["lat"], d["lng"], d["phone"],
                d["is_search_browse_enabled"], d["zone_id"], d["updated_at"],
                _ts_to_str(d["updated_at"])
            ])

    return filepath

def export_warehouse_history_csv() -> str:
    """
    Export warehouse stock history to a CSV file.
    Returns the file path of the exported CSV.
    """
    os.makedirs(CSV_DIR, exist_ok=True)
    filepath = os.path.join(CSV_DIR, "warehouse_history.csv")

    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM stock_snapshots
            WHERE store_id = 'warehouse'
            ORDER BY checked_at ASC
        """).fetchall()

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "stock_level", "product_status", "price", "mrp",
            "offer_price", "discount", "checked_at_ms", "checked_at_datetime"
        ])
        for row in rows:
            d = dict(row)
            writer.writerow([
                d["id"], d["stock_level"], d["product_status"], d["price"],
                d["mrp"], d["offer_price"], d["discount"], d["checked_at"],
                _ts_to_str(d["checked_at"])
            ])

    return filepath

def export_latest_store_availability_csv() -> str:
    """
    Export the latest per-store availability to a CSV file.
    Returns the file path of the exported CSV.
    """
    os.makedirs(CSV_DIR, exist_ok=True)
    filepath = os.path.join(CSV_DIR, "latest_store_availability.csv")

    snapshots = get_all_latest_snapshots()
    stores = {s["fp_store_id"]: s for s in get_all_cached_stores()}

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "store_id", "store_name", "address", "store_type", "lat", "lng",
            "stock_level", "product_status", "in_stock",
            "checked_at_ms", "checked_at_datetime"
        ])
        for snap in snapshots:
            if snap["store_id"] == "warehouse":
                continue
            store = stores.get(snap["store_id"], {})
            in_stock = (
                int(snap.get("stock_level", 0) or 0) > 0
                and snap.get("product_status") == "ENABLED"
            )
            writer.writerow([
                snap["store_id"], snap["store_name"],
                store.get("address", ""), store.get("store_type", ""),
                store.get("lat", ""), store.get("lng", ""),
                snap.get("stock_level"), snap.get("product_status"),
                in_stock, snap["checked_at"], _ts_to_str(snap["checked_at"])
            ])

    return filepath

def export_all_csvs() -> dict:
    """
    Export all data to CSV files. Called after each stock check cycle.
    Returns dict of {name: filepath}.
    """
    return {
        "stock_snapshots": export_snapshots_csv(),
        "store_cache": export_stores_csv(),
        "warehouse_history": export_warehouse_history_csv(),
        "latest_store_availability": export_latest_store_availability_csv(),
    }
