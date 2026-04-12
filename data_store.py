"""
Storage helpers for the FairPrice stock tracker.

By default, data is stored as CSV files in the local data/ directory.
If Google Sheets credentials are available via .streamlit/secrets.toml or
environment variables, the same API transparently uses Google Sheets instead.
"""

import csv
import io
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    import tomli as tomllib

try:
    import gspread
    from gspread.exceptions import WorksheetNotFound
except ImportError:  # pragma: no cover - optional dependency until configured
    gspread = None
    WorksheetNotFound = Exception

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
    "lat", "lng", "in_store_stock", "sap_stock", "price", "mrp", "product_sku"
]

WAREHOUSE_FIELDS = [
    "timestamp", "in_store_stock", "online_stock", "sap_stock",
    "price", "mrp", "discount", "product_name", "product_sku"
]

STORES_FIELDS = [
    "id", "name", "address", "lat", "lng", "postal_code", "store_type", "zone_id"
]

_EXPORT_FIELDS_BY_PATH = {
    STORE_STOCK_CSV: STORE_STOCK_FIELDS,
    WAREHOUSE_CSV: WAREHOUSE_FIELDS,
    STORES_CSV: STORES_FIELDS,
}

# --- In-memory cache keyed by (filepath, mtime) or worksheet title --------
_csv_cache: dict[str, tuple[float, list[dict]]] = {}
_sheet_cache: dict[str, tuple[float, list[dict]]] = {}
_gsheets_config_cache: Optional[dict] = None
_gsheets_client = None
_gsheets_workbook = None
_worksheet_initialized: set[str] = set()  # tracks worksheets whose headers have been verified
_worksheet_cache: dict[str, object] = {}  # caches worksheet objects to avoid fetch_sheet_metadata on every call


def _read_csv_cached(path: str) -> list[dict]:
    """
    Read a CSV file and cache the result. Returns cached data if the file
    hasn't been modified since last read.
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


def _rewrite_csv(path: str, fields: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _ensure_csv_schema(path: str, fields: list[str]) -> None:
    if not os.path.exists(path):
        return

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        existing_fields = reader.fieldnames or []
        if existing_fields == fields:
            return
        rows = list(reader)

    _rewrite_csv(path, fields, rows)
    invalidate_cache()


def _load_toml(path: str) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _extract_gsheets_config(raw: dict) -> dict:
    if not raw:
        return {}

    if isinstance(raw.get("gsheets"), dict):
        return dict(raw["gsheets"])

    connections = raw.get("connections", {})
    if isinstance(connections, dict) and isinstance(connections.get("gsheets"), dict):
        return dict(connections["gsheets"])

    return {}


def _get_gsheets_config() -> dict:
    global _gsheets_config_cache

    if _gsheets_config_cache is not None:
        return _gsheets_config_cache

    env_spreadsheet = (
        os.environ.get("GSHEETS_SPREADSHEET")
        or os.environ.get("GSHEETS_SPREADSHEET_URL")
        or os.environ.get("GOOGLE_SHEETS_SPREADSHEET")
    )
    env_service_account = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if env_spreadsheet and env_service_account:
        config = json.loads(env_service_account)
        config["spreadsheet"] = env_spreadsheet
        config["worksheet_store_stock"] = os.environ.get("GSHEETS_WORKSHEET_STORE_STOCK", "store_stock_history")
        config["worksheet_warehouse"] = os.environ.get("GSHEETS_WORKSHEET_WAREHOUSE", "warehouse_history")
        config["worksheet_stores"] = os.environ.get("GSHEETS_WORKSHEET_STORES", "stores")
        _gsheets_config_cache = config
        return config

    secrets_paths = [
        os.path.join(_SCRIPT_DIR, ".streamlit", "secrets.toml"),
        os.path.join(os.path.expanduser("~"), ".streamlit", "secrets.toml"),
    ]
    for path in secrets_paths:
        if not os.path.exists(path):
            continue
        config = _extract_gsheets_config(_load_toml(path))
        if config.get("spreadsheet"):
            _gsheets_config_cache = config
            return config

    try:
        import streamlit as st

        streamlit_secrets = st.secrets.to_dict()
        config = _extract_gsheets_config(streamlit_secrets)
        if config.get("spreadsheet"):
            _gsheets_config_cache = config
            return config
    except Exception:
        pass

    _gsheets_config_cache = {}
    return _gsheets_config_cache


def using_gsheets_backend() -> bool:
    """Return True when Google Sheets is configured as the persistence backend."""
    return bool(_get_gsheets_config().get("spreadsheet"))


def get_storage_backend_status() -> dict:
    """Describe which persistence backend is active and how it was configured."""
    config = _get_gsheets_config()
    secrets_paths = [
        os.path.join(_SCRIPT_DIR, ".streamlit", "secrets.toml"),
        os.path.join(os.path.expanduser("~"), ".streamlit", "secrets.toml"),
    ]
    source = "local_csv"
    if config.get("spreadsheet"):
        if (
            os.environ.get("GSHEETS_SPREADSHEET")
            or os.environ.get("GSHEETS_SPREADSHEET_URL")
            or os.environ.get("GOOGLE_SHEETS_SPREADSHEET")
        ) and os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"):
            source = "environment"
        elif any(os.path.exists(path) for path in secrets_paths):
            source = next(path for path in secrets_paths if os.path.exists(path))
        else:
            source = "streamlit_secrets"

    return {
        "backend": "google_sheets" if config.get("spreadsheet") else "local_csv",
        "configured": bool(config.get("spreadsheet")),
        "source": source,
        "spreadsheet": config.get("spreadsheet", ""),
        "data_dir": DATA_DIR,
    }


def _normalize_private_key(value: str) -> str:
    return value.replace("\\n", "\n") if value else value


def _get_gsheets_client():
    global _gsheets_client

    if _gsheets_client is not None:
        return _gsheets_client

    config = _get_gsheets_config()
    if not config:
        return None
    if gspread is None:
        raise RuntimeError("Google Sheets backend configured, but gspread is not installed.")

    credentials = {
        k: v for k, v in config.items()
        if k not in {
            "spreadsheet", "worksheet_store_stock", "worksheet_warehouse",
            "worksheet_stores", "enabled",
        }
    }
    credentials["private_key"] = _normalize_private_key(credentials.get("private_key", ""))

    _gsheets_client = gspread.service_account_from_dict(credentials)
    return _gsheets_client


def _get_gsheets_workbook():
    global _gsheets_workbook

    if _gsheets_workbook is not None:
        return _gsheets_workbook

    config = _get_gsheets_config()
    if not config:
        return None

    client = _get_gsheets_client()
    spreadsheet = config["spreadsheet"]
    if spreadsheet.startswith("https://"):
        _gsheets_workbook = client.open_by_url(spreadsheet)
    else:
        _gsheets_workbook = client.open_by_key(spreadsheet)
    return _gsheets_workbook


def _worksheet_title(kind: str) -> str:
    config = _get_gsheets_config()
    return config.get(f"worksheet_{kind}", {
        "store_stock": "store_stock_history",
        "warehouse": "warehouse_history",
        "stores": "stores",
    }[kind])


def _get_worksheet(title: str, fields: list[str], create_if_missing: bool) -> Optional[object]:
    workbook = _get_gsheets_workbook()
    if workbook is None:
        return None

    if title not in _worksheet_cache:
        try:
            worksheet = workbook.worksheet(title)
            # Existing worksheet — assume headers are already correct, no read needed
            _worksheet_initialized.add(title)
        except WorksheetNotFound:
            if not create_if_missing:
                return None
            worksheet = workbook.add_worksheet(title=title, rows=1000, cols=max(len(fields), 8))
            if fields:
                worksheet.append_row(fields, value_input_option="RAW")
            _worksheet_initialized.add(title)
        _worksheet_cache[title] = worksheet

    return _worksheet_cache[title]


def _ensure_sheet_schema(title: str, fields: list[str]) -> Optional[object]:
    worksheet = _get_worksheet(title, fields, create_if_missing=True)
    if worksheet is None:
        return None

    # Schema is verified at worksheet creation time; no extra reads needed
    if title in _worksheet_initialized:
        return worksheet

    existing_fields = worksheet.row_values(1)
    if existing_fields == fields:
        _worksheet_initialized.add(title)
        return worksheet

    values = worksheet.get_all_values()
    headers = values[0] if values else []
    rows = []
    for raw_row in values[1:]:
        row = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row[header] = raw_row[idx] if idx < len(raw_row) else ""
        rows.append(row)

    worksheet.clear()
    worksheet.append_row(fields, value_input_option="RAW")
    if rows:
        worksheet.append_rows(
            [[row.get(field, "") for field in fields] for row in rows],
            value_input_option="RAW",
        )
    invalidate_cache()
    return worksheet


def _rows_from_sheet(title: str, fields: list[str]) -> list[dict]:
    cached = _sheet_cache.get(title)
    now = time.time()
    if cached and now - cached[0] < 120:
        return cached[1]

    worksheet = _get_worksheet(title, fields, create_if_missing=False)
    if worksheet is None:
        return []

    values = worksheet.get_all_values()
    if not values:
        return []

    headers = values[0]
    rows = []
    for raw_row in values[1:]:
        if not any(cell.strip() for cell in raw_row):
            continue
        row = {}
        for idx, header in enumerate(headers):
            if not header:
                continue
            row[header] = raw_row[idx] if idx < len(raw_row) else ""
        rows.append(row)

    _sheet_cache[title] = (now, rows)
    return rows


def _append_sheet_row(title: str, fields: list[str], row: dict) -> None:
    worksheet = _ensure_sheet_schema(title, fields)
    if worksheet is None:
        raise RuntimeError("Google Sheets backend is not available.")

    values = [row.get(field, "") for field in fields]
    worksheet.append_row(values, value_input_option="RAW")
    invalidate_cache()


def _replace_sheet_rows(title: str, fields: list[str], rows: list[dict]) -> None:
    worksheet = _ensure_sheet_schema(title, fields)
    if worksheet is None:
        raise RuntimeError("Google Sheets backend is not available.")

    worksheet.clear()
    worksheet.append_row(fields, value_input_option="RAW")
    if rows:
        worksheet.append_rows(
            [[row.get(field, "") for field in fields] for row in rows],
            value_input_option="RAW",
        )
    invalidate_cache()


def _read_store_stock_rows() -> list[dict]:
    if using_gsheets_backend():
        return _rows_from_sheet(_worksheet_title("store_stock"), STORE_STOCK_FIELDS)
    return _read_csv_cached(STORE_STOCK_CSV)


def _read_warehouse_rows() -> list[dict]:
    if using_gsheets_backend():
        return _rows_from_sheet(_worksheet_title("warehouse"), WAREHOUSE_FIELDS)
    return _read_csv_cached(WAREHOUSE_CSV)


def _read_store_rows() -> list[dict]:
    if using_gsheets_backend():
        return _rows_from_sheet(_worksheet_title("stores"), STORES_FIELDS)
    return _read_csv_cached(STORES_CSV)


def _filter_rows_by_product(rows: list[dict], product_sku: Optional[str]) -> list[dict]:
    if not product_sku:
        return rows
    return [row for row in rows if str(row.get("product_sku", "")).strip() == product_sku]


def _csv_text_from_rows(fields: list[str], rows: list[dict]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return output.getvalue()


def read_csv_export(path: str) -> Optional[str]:
    """Return CSV text for downloads, from local files or Google Sheets."""
    if not using_gsheets_backend():
        if os.path.exists(path):
            with open(path, "r") as f:
                return f.read()
        return None

    fields = _EXPORT_FIELDS_BY_PATH.get(path)
    if fields is None:
        return None
    if path == STORE_STOCK_CSV:
        rows = _read_store_stock_rows()
    elif path == WAREHOUSE_CSV:
        rows = _read_warehouse_rows()
    else:
        rows = _read_store_rows()
    return _csv_text_from_rows(fields, rows)


def invalidate_cache():
    """Clear in-memory caches (call after writes)."""
    _csv_cache.clear()
    _sheet_cache.clear()
    # Do NOT clear _worksheet_cache or _worksheet_initialized — worksheet objects
    # remain valid after writes and re-fetching them costs API quota.


# --- Directory & init -----------------------------------------

def ensure_data_dir():
    if using_gsheets_backend():
        return
    os.makedirs(DATA_DIR, exist_ok=True)


def _init_csv(path, fields):
    """Create CSV with header if it doesn't exist."""
    if using_gsheets_backend():
        return
    if not os.path.exists(path):
        ensure_data_dir()
        _rewrite_csv(path, fields, [])
        return
    _ensure_csv_schema(path, fields)


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
    """Save the normalized store list to CSV or Google Sheets."""
    rows = []
    for s in stores:
        rows.append({
            "id": s.get("id", ""),
            "name": s.get("name", ""),
            "address": s.get("address", ""),
            "lat": s.get("lat", ""),
            "lng": s.get("lng", ""),
            "postal_code": s.get("postalCode", ""),
            "store_type": s.get("storeType", ""),
            "zone_id": s.get("zoneId", ""),
        })

    if using_gsheets_backend():
        _replace_sheet_rows(_worksheet_title("stores"), STORES_FIELDS, rows)
        return

    ensure_data_dir()
    with open(STORES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STORES_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    invalidate_cache()


# --- Warehouse ------------------------------------------------

def append_warehouse_snapshot(data):
    """Append a warehouse stock snapshot."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "timestamp": now,
        "in_store_stock": data.get("in_store_stock", 0),
        "online_stock": data.get("online_stock", 0),
        "sap_stock": data.get("sap_stock", 0),
        "price": data.get("price", 0),
        "mrp": data.get("mrp", 0),
        "discount": data.get("discount", 0),
        "product_name": data.get("product_name", ""),
        "product_sku": data.get("product_sku", ""),
    }

    if using_gsheets_backend():
        _append_sheet_row(_worksheet_title("warehouse"), WAREHOUSE_FIELDS, row)
        return

    _init_csv(WAREHOUSE_CSV, WAREHOUSE_FIELDS)
    with open(WAREHOUSE_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=WAREHOUSE_FIELDS)
        writer.writerow(row)
    invalidate_cache()


def read_warehouse_history(product_sku: Optional[str] = None):
    """Read all warehouse history from the active backend."""
    rows = _filter_rows_by_product(_read_warehouse_rows(), product_sku)
    parsed = []
    for r in rows:
        row = dict(r)
        for k in ["in_store_stock", "online_stock", "sap_stock"]:
            row[k] = int(float(row.get(k, 0) or 0))
        for k in ["price", "mrp", "discount"]:
            row[k] = float(row.get(k, 0) or 0)
        row["product_sku"] = str(row.get("product_sku", "") or "")
        parsed.append(row)
    return parsed


def get_latest_warehouse(product_sku: Optional[str] = None):
    """Get the most recent warehouse snapshot."""
    history = read_warehouse_history(product_sku=product_sku)
    if history:
        return history[-1]
    return None


# --- Store stock ----------------------------------------------

def append_store_stock(batch_id, store_id, store_name, store_type, address, lat, lng,
                       in_store_stock, sap_stock, price, mrp, product_sku=""):
    """Append a per-store stock snapshot with batch_id for grouping."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = {
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
        "product_sku": product_sku,
    }

    if using_gsheets_backend():
        _append_sheet_row(_worksheet_title("store_stock"), STORE_STOCK_FIELDS, row)
        return

    _init_csv(STORE_STOCK_CSV, STORE_STOCK_FIELDS)
    with open(STORE_STOCK_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STORE_STOCK_FIELDS)
        writer.writerow(row)
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
        row["product_sku"] = str(row.get("product_sku", "") or "")
        parsed.append(row)
    return parsed


def read_store_stock_history(product_sku: Optional[str] = None):
    """Read all store stock history from the active backend."""
    return _parse_store_stock_rows(_filter_rows_by_product(_read_store_stock_rows(), product_sku))


def get_latest_store_stock(product_sku: Optional[str] = None):
    """
    Get the most recent stock snapshot for each store.
    Optimized: scans from the end of the active dataset to find the latest batch,
    then only parses rows from that batch.
    """
    rows = _filter_rows_by_product(_read_store_stock_rows(), product_sku)
    if not rows:
        return {}

    latest_batch = None
    for r in reversed(rows):
        bid = r.get("batch_id", "")
        if bid:
            latest_batch = bid
            break

    if not latest_batch:
        return {}

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
            parsed["product_sku"] = str(parsed.get("product_sku", "") or "")
            latest[str(parsed["store_id"])] = parsed

    return latest


def get_batch_ids(product_sku: Optional[str] = None):
    """
    Get all unique batch IDs sorted by time (newest first).
    Optimized: only reads the batch_id column.
    """
    rows = _filter_rows_by_product(_read_store_stock_rows(), product_sku)
    batches = sorted(
        set(r.get("batch_id", "") for r in rows if r.get("batch_id")),
        reverse=True,
    )
    return batches


def get_batch_data(batch_id, product_sku: Optional[str] = None):
    """Get all store stock data for a specific batch."""
    batch_rows = [
        r for r in _filter_rows_by_product(_read_store_stock_rows(), product_sku)
        if r.get("batch_id") == batch_id
    ]
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
