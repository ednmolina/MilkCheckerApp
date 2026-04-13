"""
FairPrice Meiji Milk Stock Tracker - Streamlit Dashboard
Shows real per-store stock counts on an OpenStreetMap with filters.
All data stored as CSV files - no database.

Optimizations:
- All data reads cached with @st.cache_data(ttl=60)
- Postal code search cached with @st.cache_data(ttl=300) to avoid repeat API calls
- Map rendered with returned_objects=[] to prevent re-renders on zoom/pan
- Form-based search to prevent re-runs on keystroke
"""

from __future__ import annotations

import streamlit as st
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium
import pandas as pd
import time
import os
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_store import (
    load_stores, get_latest_store_stock, get_latest_warehouse,
    read_warehouse_history, read_store_stock_history,
    get_batch_ids, get_batch_data,
    haversine_km, read_csv_export, get_storage_backend_status,
    parse_stored_timestamp, format_timestamp_for_timezone,
    STORE_STOCK_CSV, WAREHOUSE_CSV, STORES_CSV,
)
from fairprice_api import PRODUCTS, search_by_postal_code
from stock_job import run_stock_check

# --- Page config ---------------------------------------------------------
st.set_page_config(
    page_title="FairPrice Meiji Protein Milk Tracker",
    page_icon="🥛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Add premium dark mode styling with mobile optimization & elegant typography
st.markdown("""
    <style>
        /* Typography & Layout - iPhone 16+ Optimized */
        body {
            -webkit-font-smoothing: antialiased;
        }

        /* Hero header styling - Refined & Minimal */
        h1 {
            color: #F5F5F7;
            padding: 1rem 0;
            text-align: center;
            font-weight: 700;
            letter-spacing: -0.02em;
            background: transparent;
            box-shadow: none;
            font-size: clamp(1.8rem, 4vw, 2.5rem);
        }
        
        /* Subheaders */
        h2, h3 {
            color: #E8E8ED;
            font-weight: 600;
            letter-spacing: -0.01em;
            margin-top: 1.5rem;
        }
        
        /* Metric Typography & Container - Apple Dark Mode Style */
        [data-testid="metric-container"] {
            background: linear-gradient(180deg, #2C2C2E 0%, #1C1C1E 100%);
            padding: 16px 20px;
            border-radius: 16px; /* Squircle feel */
            border: 1px solid rgba(10, 132, 255, 0.3); /* Subtle blue hint */
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s ease;
        }
        [data-testid="metric-container"]:hover {
            transform: translateY(-2px);
        }
        [data-testid="stMetricLabel"] {
            color: #A1A1A6;
            font-size: 0.9rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 4px;
        }
        [data-testid="stMetricValue"] {
            color: #F5F5F7;
            font-size: 2rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }
        
        /* Sidebar enhancement - Apple Elevated Surface */
        [data-testid="stSidebar"] {
            background-color: #1C1C1E !important;
            border-right: 1px solid #3A3A3C;
        }
        
        /* Tab Styling - iOS Segmented Control feel */
        [data-testid="stTabs"] [data-baseweb="tab-list"] {
            background-color: #2C2C2E;
            border-radius: 12px;
            padding: 4px;
            gap: 4px;
            border-bottom: none;
        }
        [data-testid="stTabs"] [data-baseweb="tab"] {
            background-color: transparent;
            border-radius: 8px;
            color: #A1A1A6;
            font-weight: 500;
            padding: 8px 16px;
            border: none !important;
            box-shadow: none;
        }
        [data-testid="stTabs"] [aria-selected="true"] {
            background-color: #48484A !important;
            color: #F5F5F7 !important;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2) !important;
            font-weight: 600;
        }
        
        /* Button styling - Sleek & Modern iOS Style */
        .stButton > button {
            background-color: #0A84FF !important;
            color: white !important;
            border: none !important;
            border-radius: 12px !important;
            font-weight: 600 !important;
            padding: 12px 24px !important;
            box-shadow: 0 4px 12px rgba(10, 132, 255, 0.3) !important;
            transition: all 0.2s ease !important;
            width: 100%;
        }
        .stButton > button:hover {
            background-color: #0071E3 !important;
            transform: scale(0.98);
        }
        
        /* Subtle dividers */
        hr {
            border: 0;
            height: 1px;
            background: #3A3A3C;
            margin: 24px 0;
        }

        /* Mobile specific adjustments */
        @media (max-width: 768px) {
            [data-testid="stMetricValue"] {
                font-size: 1.5rem;
            }
            [data-testid="metric-container"] {
                padding: 12px 16px;
            }
            .stButton > button {
                padding: 14px 20px !important;
                font-size: 1.1rem !important; /* Larger touch target for iPhone */
            }
        }
    </style>
""", unsafe_allow_html=True)

SG_CENTER = [1.3521, 103.8198]
_UTC = ZoneInfo("UTC")
DEFAULT_PRODUCT_KEY = "chocolate"
PRODUCT_KEYS = list(PRODUCTS.keys())

if st.session_state.get("selected_product_key") not in PRODUCTS:
    st.session_state["selected_product_key"] = DEFAULT_PRODUCT_KEY

selected_product_key = st.session_state["selected_product_key"]
selected_product = PRODUCTS[selected_product_key]
selected_product_sku = selected_product["sku"]
selected_product_name = selected_product["name"]

# --- Timezone setup -----------------------------------------------------
_TZ_OPTIONS = {"🇺🇸 New York": "America/New_York", "🇸🇬 Singapore": "Asia/Singapore"}
_DEFAULT_TZ_LABEL = "🇺🇸 New York"

if st.session_state.get("display_tz") not in _TZ_OPTIONS:
    st.session_state["display_tz"] = _DEFAULT_TZ_LABEL

_tz_label = st.session_state["display_tz"]
_display_tz = ZoneInfo(_TZ_OPTIONS[_tz_label])

# --- Helpers -------------------------------------------------------------
def stock_color(stock: int) -> str:
    if stock < 0:
        return "gray"
    if stock == 0:
        return "red"
    if stock <= 10:
        return "orange"
    return "green"

def stock_icon(stock: int) -> str:
    if stock < 0:
        return "question-sign"
    if stock == 0:
        return "remove"
    return "ok"

def stock_label(stock: int) -> str:
    if stock < 0:
        return "Not Available"
    if stock == 0:
        return "Out of Stock"
    return f"{stock} units"


def convert_timestamp_series(values: pd.Series, target_tz: ZoneInfo) -> pd.Series:
    """Parse mixed legacy/current timestamp strings and convert for charting."""
    parsed = values.apply(parse_stored_timestamp)
    return pd.to_datetime(parsed, utc=True, errors="coerce").dt.tz_convert(target_tz)


def prepare_store_history_frame(history_rows: list[dict]) -> pd.DataFrame:
    """
    Build a deduplicated per-store history frame.
    Some batches may contain duplicate rows for the same store; keep the latest
    row within each (batch_id, store_id) pair before computing analytics.
    """
    if not history_rows:
        return pd.DataFrame()

    df = pd.DataFrame(history_rows).copy()
    df["timestamp"] = convert_timestamp_series(df["timestamp"], _UTC)
    df["store_id"] = df["store_id"].astype(str)
    df = df.sort_values(["batch_id", "store_id", "timestamp"])
    df = df.drop_duplicates(subset=["batch_id", "store_id"], keep="last")
    return df.sort_values(["timestamp", "store_id"]).reset_index(drop=True)


def infer_store_movements(df_store: pd.DataFrame) -> pd.DataFrame:
    """
    Infer lower-bound sales and restocks from consecutive snapshots.
    - A drop means at least that many units were sold.
    - A rise means at least that many units were restocked.
    Exact sales cannot be known when restocks happen between snapshots.
    """
    if df_store.empty:
        return pd.DataFrame()

    movement_df = df_store.sort_values(["store_id", "timestamp", "batch_id"]).copy()
    movement_df["prev_stock"] = movement_df.groupby("store_id")["in_store_stock"].shift(1)
    movement_df["prev_batch_id"] = movement_df.groupby("store_id")["batch_id"].shift(1)

    comparable = (
        movement_df["prev_stock"].notna()
        & (movement_df["prev_stock"] >= 0)
        & (movement_df["in_store_stock"] >= 0)
    )

    movement_df["min_units_sold"] = 0
    movement_df["min_units_restocked"] = 0
    movement_df["net_change"] = 0

    movement_df.loc[comparable, "min_units_sold"] = (
        movement_df.loc[comparable, "prev_stock"] - movement_df.loc[comparable, "in_store_stock"]
    ).clip(lower=0).astype(int)
    movement_df.loc[comparable, "min_units_restocked"] = (
        movement_df.loc[comparable, "in_store_stock"] - movement_df.loc[comparable, "prev_stock"]
    ).clip(lower=0).astype(int)
    movement_df.loc[comparable, "net_change"] = (
        movement_df.loc[comparable, "in_store_stock"] - movement_df.loc[comparable, "prev_stock"]
    ).astype(int)

    return movement_df


def build_batch_totals(df_store: pd.DataFrame, movement_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inventory and inferred movement per batch."""
    if df_store.empty:
        return pd.DataFrame()

    batch_totals = []
    movement_by_batch = {}
    if not movement_df.empty:
        movement_by_batch = (
            movement_df.groupby("batch_id", as_index=False)[
                ["min_units_sold", "min_units_restocked", "net_change"]
            ]
            .sum()
            .set_index("batch_id")
            .to_dict("index")
        )

    for batch_id, batch_rows in df_store.groupby("batch_id", sort=False):
        available_rows = batch_rows[batch_rows["in_store_stock"] >= 0]
        totals = movement_by_batch.get(
            batch_id,
            {"min_units_sold": 0, "min_units_restocked": 0, "net_change": 0},
        )
        total_units = int(available_rows["in_store_stock"].sum())
        stores_with_product = len(available_rows)

        batch_totals.append({
            "timestamp": batch_rows["timestamp"].min(),
            "batch_id": batch_id,
            "total_units": total_units,
            "stores_checked": len(batch_rows),
            "stores_with_product": stores_with_product,
            "stores_in_stock": len(available_rows[available_rows["in_store_stock"] > 0]),
            "stores_out_of_stock": len(available_rows[available_rows["in_store_stock"] == 0]),
            "avg_stock_per_store": round(total_units / stores_with_product, 1) if stores_with_product else 0,
            "min_units_sold": int(totals["min_units_sold"]),
            "min_units_restocked": int(totals["min_units_restocked"]),
            "net_change": int(totals["net_change"]),
        })

    df_totals = pd.DataFrame(batch_totals)
    return df_totals.sort_values("timestamp").reset_index(drop=True)


def build_store_movement_summary(movement_df: pd.DataFrame) -> pd.DataFrame:
    """Summarize inferred movement for each store across the full history."""
    if movement_df.empty:
        return pd.DataFrame()

    latest_by_store = (
        movement_df.sort_values(["store_id", "timestamp", "batch_id"])
        .groupby("store_id", as_index=False)
        .tail(1)[["store_id", "store_name", "store_type", "in_store_stock", "timestamp"]]
        .rename(columns={
            "in_store_stock": "latest_stock",
            "timestamp": "last_seen",
        })
    )

    summary = (
        movement_df.groupby("store_id", as_index=False)
        .agg(
            first_seen=("timestamp", "min"),
            observations=("batch_id", "nunique"),
            min_units_sold=("min_units_sold", "sum"),
            min_units_restocked=("min_units_restocked", "sum"),
            net_change=("net_change", "sum"),
        )
        .merge(latest_by_store, on="store_id", how="left")
        .sort_values(["min_units_sold", "latest_stock"], ascending=[False, False])
        .reset_index(drop=True)
    )

    return summary

# --- Cached data loaders -------------------------------------------------
@st.cache_data(ttl=60)
def cached_load_stores():
    return load_stores()

@st.cache_data(ttl=60)
def cached_get_latest_store_stock(product_sku: str):
    return get_latest_store_stock(product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_get_latest_warehouse(product_sku: str):
    return get_latest_warehouse(product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_warehouse_history(product_sku: str):
    return read_warehouse_history(product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_store_stock_history(product_sku: str):
    return read_store_stock_history(product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_batch_ids(product_sku: str):
    return get_batch_ids(product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_batch_data(batch_id: str, product_sku: str):
    return get_batch_data(batch_id, product_sku=product_sku)

@st.cache_data(ttl=60)
def cached_all_warehouse_history():
    return read_warehouse_history()

@st.cache_data(ttl=60)
def cached_all_store_stock_history():
    return read_store_stock_history()

@st.cache_data(ttl=60)
def cached_storage_status():
    return get_storage_backend_status()

@st.cache_data(ttl=300)
def cached_postal_search(postal_code: str):
    """Cache postal code searches for 5 minutes to avoid repeat API calls."""
    return search_by_postal_code(postal_code)

@st.cache_data(ttl=60)
def cached_export_csv(path: str):
    """Cache CSV exports for download buttons."""
    return read_csv_export(path)

# --- Pre-compute store stock data (used by sidebar + tabs) --------------
all_stores = cached_load_stores()
latest_stock = cached_get_latest_store_stock(selected_product_sku)
all_store_history = cached_all_store_stock_history()
store_history = cached_store_stock_history(selected_product_sku)
all_warehouse_history = cached_all_warehouse_history()
warehouse_history = cached_warehouse_history(selected_product_sku)
storage_status = cached_storage_status()
store_history_df = prepare_store_history_frame(store_history)
store_movement_df = infer_store_movements(store_history_df)
store_movement_summary = build_store_movement_summary(store_movement_df)
inventory_totals_df = build_batch_totals(store_history_df, store_movement_df)
legacy_product_data_present = bool((all_store_history or all_warehouse_history) and not (store_history or warehouse_history))

stores_with_stock = []
for store in all_stores:
    if not store.get("lat") or not store.get("lng"):
        continue
    sid = str(store["id"])
    stock_data = latest_stock.get(sid, {})
    stock_count = stock_data.get("in_store_stock", -1) if stock_data else -1
    stores_with_stock.append({
        **store,
        "stock": stock_count,
        "last_checked": stock_data.get("timestamp", "Never") if stock_data else "Never",
    })

# Compute totals
total_units_all_stores = sum(s["stock"] for s in stores_with_stock if s["stock"] > 0)
in_stock_count = len([s for s in stores_with_stock if s["stock"] > 0])
out_stock_count = len([s for s in stores_with_stock if s["stock"] == 0])
low_stock_count = len([s for s in stores_with_stock if 0 < s["stock"] <= 10])
not_avail_count = len([s for s in stores_with_stock if s["stock"] < 0])

# --- Sidebar ------------------------------------------------------------
with st.sidebar:
    st.title("🥛 Meiji Protein Milk Tracker")
    st.selectbox(
        "Product",
        PRODUCT_KEYS,
        index=PRODUCT_KEYS.index(selected_product_key),
        format_func=lambda key: PRODUCTS[key]["name"],
        key="selected_product_key",
    )
    st.caption(selected_product_name)
    backend_label = "Google Sheets" if storage_status["backend"] == "google_sheets" else "Local CSV"
    st.caption(f"Backend: {backend_label}")
    if storage_status["backend"] == "google_sheets":
        st.caption(f"Source: {storage_status['source']}")
    else:
        st.caption(f"Path: {storage_status['data_dir']}")
    st.divider()

    # Warehouse stock summary
    warehouse = cached_get_latest_warehouse(selected_product_sku)
    if warehouse:
        st.metric("Warehouse Stock", f"{warehouse['in_store_stock']} units")
        price = warehouse.get("price", 0)
        mrp = warehouse.get("mrp", 0)
        discount = warehouse.get("discount", 0)
        if price:
            col1, col2 = st.columns(2)
            col1.metric("Sale Price", f"${float(price):.2f}")
            col2.metric("MRP", f"${float(mrp):.2f}")
            if discount and float(discount) < 0:
                st.success(f"Save ${abs(float(discount)):.2f}")
        st.caption(
            f"Last checked on: "
            f"{format_timestamp_for_timezone(warehouse.get('timestamp', 'N/A'), _display_tz)}"
        )
    else:
        st.warning("⚠️ No warehouse data yet. Run a stock check first.")

    if legacy_product_data_present:
        st.warning(f"⚠️ Only other-product history is available. Run a fresh stock check to populate {selected_product_name} data.")

    st.divider()

    # Total units across all stores
    st.metric("Total Store Inventory", f"{total_units_all_stores:,} units")
    st.caption(f"Across {in_stock_count} stores with stock")

    st.divider()

    # Map filters
    st.subheader("🗺️ Map Filters")
    filter_option = st.radio(
        "Show stores:",
        ["All Stores", "In Stock (> 0)", "Out of Stock (0)", "Low Stock (1-10)", "Not Available"],
        key="map_filter",
    )

    st.divider()

    # Stock check controls
    st.subheader("🔄 Stock Check")
    if st.button("Run Stock Check Now", use_container_width=True, type="primary"):
        with st.spinner("Checking both products... (this takes ~60-120s)"):
            result = run_stock_check(verbose=False)
        if result.get("success"):
            st.success(result["message"])
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
        else:
            st.error(result.get("message", "Failed"))

    # Show batch info (cached)
    batches = cached_batch_ids(selected_product_sku)
    if batches:
        st.caption(f"Latest batch: {batches[0]}")
        st.caption(f"Total batches: {len(batches)}")

    st.divider()

    # CSV downloads (cached file reads)
    st.subheader("📥 Download Data")
    for label, path in [
        ("Store Stock History", STORE_STOCK_CSV),
        ("Warehouse History", WAREHOUSE_CSV),
        ("Store List", STORES_CSV),
    ]:
        content = cached_export_csv(path)
        if content:
            st.download_button(
                label=label,
                data=content,
                file_name=os.path.basename(path),
                mime="text/csv",
                use_container_width=True,
            )

    st.divider()
    if storage_status["backend"] == "google_sheets":
        st.caption("Google Sheets sync is active for this app instance.")
    else:
        st.caption("Google Sheets is not configured for this app instance. Writes are going to local CSV files.")
    st.caption("Stock data from FairPrice product/v2 API")

# --- Timezone toggle (top of page, mobile-friendly) ---------------------
tz_col, _ = st.columns([2, 5])
with tz_col:
    st.radio(
        "Display timezone",
        list(_TZ_OPTIONS.keys()),
        horizontal=True,
        label_visibility="collapsed",
        key="display_tz",
    )


# --- Main content -------------------------------------------------------
st.markdown("# 🥛 FairPrice Meiji Tracker")
st.markdown(f"**Tracking:** {selected_product_name}")

col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.metric("Total Units Across All Stores", f"{total_units_all_stores:,}")

tab_map, tab_history, tab_inventory = st.tabs([
    "📍 Store Finder", "📊 Stock History", "📈 Total Inventory"
])

if legacy_product_data_present:
    st.warning(
        f"No stored history exists yet for {selected_product_name}. "
        "Run `Run Stock Check Now` once to create a fresh baseline."
    )

# --- Tab 1: Store Finder -----------------------------------------------
with tab_map:
    st.header("Store Finder")
    st.caption(selected_product_name)

    # Postal code search - form prevents re-runs on keystroke
    with st.form("search_form", clear_on_submit=False):
        col_search, col_btn = st.columns([3, 1])
        with col_search:
            postal_code = st.text_input(
                "Enter Singapore postal code",
                placeholder="e.g. 520101",
                label_visibility="collapsed",
            )
        with col_btn:
            search_clicked = st.form_submit_button("Search", use_container_width=True)

    # Apply filter
    def apply_filter(stores, filter_opt):
        if filter_opt == "In Stock (> 0)":
            return [s for s in stores if s["stock"] > 0]
        elif filter_opt == "Out of Stock (0)":
            return [s for s in stores if s["stock"] == 0]
        elif filter_opt == "Low Stock (1-10)":
            return [s for s in stores if 0 < s["stock"] <= 10]
        elif filter_opt == "Not Available":
            return [s for s in stores if s["stock"] < 0]
        return stores

    # Determine what to show on map
    search_lat, search_lng = None, None
    nearby_stores = None

    if search_clicked and postal_code.strip():
        try:
            result = cached_postal_search(postal_code.strip())
            addresses = result.get("addresses", [])
            if addresses:
                addr = addresses[0]
                search_lat = addr.get("lat")
                search_lng = addr.get("long")
        except Exception as e:
            st.error(f"Search failed: {e}")

    if search_lat and search_lng:
        for s in stores_with_stock:
            s["distance_km"] = haversine_km(
                search_lat, search_lng, float(s["lat"]), float(s["lng"])
            )

        nearby_stores = sorted(stores_with_stock, key=lambda x: x.get("distance_km", 0))
        filtered = apply_filter(nearby_stores, filter_option)
        map_center = [search_lat, search_lng]
        zoom = 14
    else:
        filtered = apply_filter(stores_with_stock, filter_option)
        map_center = SG_CENTER
        zoom = 12

    # Summary metrics
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    metrics = [
        (col1, "📦", "Total Stores", len(stores_with_stock)),
        (col2, "🟢", "In Stock", in_stock_count),
        (col3, "🟡", "Low Stock", low_stock_count),
        (col4, "🔴", "Out of Stock", out_stock_count),
        (col5, "⚪", "Not Available", not_avail_count),
        (col6, "📊", "Total Units", f"{total_units_all_stores:,}"),
    ]
    for col, emoji, label, value in metrics:
        col.metric(f"{emoji} {label}", value)

    st.caption(f"Showing {len(filtered)} stores (filter: {filter_option})")

    # Build map
    m = folium.Map(location=map_center, zoom_start=zoom, tiles="OpenStreetMap")

    if search_lat and search_lng:
        folium.Marker(
            [search_lat, search_lng],
            popup=f"Your location: {postal_code}",
            icon=folium.Icon(color="blue", icon="home"),
        ).add_to(m)

    marker_cluster = MarkerCluster(
        name="Stores",
        options={"maxClusterRadius": 40, "disableClusteringAtZoom": 14},
    ).add_to(m)

    for store in filtered:
        stock = store["stock"]
        color = stock_color(stock)
        icon = stock_icon(stock)
        label = stock_label(stock)

        popup_html = f"""
        <div style="min-width:200px">
            <b>{store.get('name', 'Unknown')}</b><br>
            <small>{store.get('storeType', '')}</small><br>
            <hr style="margin:4px 0">
            <b>Stock: {label}</b><br>
            <small>{store.get('address', '')}</small><br>
            <small>Last checked on: {format_timestamp_for_timezone(store.get('last_checked', 'Never'), _display_tz)}</small>
        </div>
        """

        folium.Marker(
            [float(store["lat"]), float(store["lng"])],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{store.get('name', '')}: {label}",
            icon=folium.Icon(color=color, icon=icon, prefix="glyphicon"),
        ).add_to(marker_cluster)

    st_folium(m, width=None, height=500, use_container_width=True, returned_objects=[])

    # Store list below map
    if nearby_stores and search_lat:
        st.subheader(f"Stores near {postal_code}")
        display_stores = apply_filter(nearby_stores[:20], filter_option)
    else:
        st.subheader("All Stores")
        display_stores = filtered

    if display_stores:
        rows = []
        for s in display_stores[:50]:
            stock = s["stock"]
            if stock > 10:
                status_badge = "🟢 In Stock"
            elif stock > 0:
                status_badge = "🟡 Low Stock"
            elif stock == 0:
                status_badge = "🔴 Out of Stock"
            else:
                status_badge = "⚪ Unknown"

            row = {
                "Store": s.get("name", "Unknown"),
                "Type": s.get("storeType", ""),
                "Stock Status": status_badge,
                "Units": stock if stock >= 0 else None,
                "Address": s.get("address", ""),
                "Last Checked On": format_timestamp_for_timezone(s.get("last_checked", "Never"), _display_tz),
            }
            if "distance_km" in s:
                row["Distance"] = f"{s['distance_km']:.1f} km"
            rows.append(row)

        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Stock Status": st.column_config.TextColumn(width="small"),
                "Units": st.column_config.NumberColumn(format="%d units"),
                "Distance": st.column_config.TextColumn(width="small"),
            }
        )
    else:
        st.info("ℹ️ No stores match the current filter.")

# --- Tab 2: Stock History ----------------------------------------------
with tab_history:
    st.header("Stock History")
    st.caption(selected_product_name)

    # Warehouse history chart (cached read)
    wh_history = warehouse_history
    if wh_history:
        st.subheader("Warehouse Stock Over Time")
        df_wh = pd.DataFrame(wh_history)
        df_wh["timestamp"] = convert_timestamp_series(df_wh["timestamp"], _display_tz)
        df_wh = df_wh.sort_values("timestamp")

        st.line_chart(df_wh.set_index("timestamp")[["in_store_stock"]], use_container_width=True)

        st.subheader("Price History")
        st.line_chart(df_wh.set_index("timestamp")[["price", "mrp"]], use_container_width=True)

        st.subheader("Warehouse Data Table")
        st.dataframe(
            df_wh[["timestamp", "in_store_stock", "online_stock", "sap_stock", "price", "mrp", "discount"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Timestamp", format="MMM DD, h:mm a"),
                "in_store_stock": st.column_config.NumberColumn("In-Store Stock", format="%d units"),
                "online_stock": st.column_config.NumberColumn("Online Stock", format="%d units"),
                "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                "mrp": st.column_config.NumberColumn("MRP", format="$%.2f"),
            }
        )
    else:
        st.info("ℹ️ No warehouse history yet. Run a stock check first.")

    st.divider()

    # Per-store stock history (cached read)
    if not store_history_df.empty:
        st.subheader("Per-Store Stock History")

        store_names = sorted(store_history_df[store_history_df["in_store_stock"] >= 0]["store_name"].unique())
        if store_names:
            selected_store = st.selectbox("Select a store:", store_names)
            store_data = store_history_df[store_history_df["store_name"] == selected_store].sort_values("timestamp").copy()
            store_data["timestamp"] = convert_timestamp_series(store_data["timestamp"], _display_tz)
            store_moves = (
                store_movement_df[store_movement_df["store_name"] == selected_store]
                .sort_values("timestamp")
                .copy()
            )
            store_moves["timestamp"] = store_moves["timestamp"].dt.tz_convert(_display_tz)

            if not store_data.empty:
                latest_store_row = store_data.iloc[-1]
                total_min_sold = int(store_moves["min_units_sold"].sum())
                total_min_restocked = int(store_moves["min_units_restocked"].sum())
                net_change = int(store_moves["net_change"].sum())

                hc1, hc2, hc3, hc4 = st.columns(4)
                hc1.metric("Latest Stock", int(latest_store_row["in_store_stock"]))
                hc2.metric("Min Sold Observed", total_min_sold)
                hc3.metric("Min Restocked", total_min_restocked)
                hc4.metric("Net Change", net_change)

                st.caption("These are lower-bound estimates from consecutive snapshots. Restocks between checks can hide additional sales.")

                st.line_chart(
                    store_data.set_index("timestamp")[["in_store_stock"]],
                    use_container_width=True,
                )
                st.dataframe(
                    store_moves[[
                        "timestamp", "batch_id", "in_store_stock", "min_units_sold",
                        "min_units_restocked", "net_change", "sap_stock", "price", "mrp"
                    ]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "timestamp": st.column_config.DatetimeColumn("Timestamp", format="MMM DD, h:mm a"),
                        "in_store_stock": st.column_config.NumberColumn("Stock", format="%d units"),
                        "min_units_sold": st.column_config.NumberColumn("Min Sold", format="%d units"),
                        "min_units_restocked": st.column_config.NumberColumn("Restocked", format="%d units"),
                        "net_change": st.column_config.NumberColumn("Net Change", format="%+d units"),
                        "price": st.column_config.NumberColumn("Price", format="$%.2f"),
                    }
                )

    st.divider()

    # Batch comparison (cached)
    st.subheader("Batch Comparison")
    batches = cached_batch_ids(selected_product_sku)
    if batches:
        selected_batch = st.selectbox("Select a batch:", batches, index=0)
        batch_data = cached_batch_data(selected_batch, selected_product_sku)
        if batch_data:
            df_batch = prepare_store_history_frame(batch_data)
            in_stock_b = len(df_batch[df_batch["in_store_stock"] > 0])
            out_b = len(df_batch[df_batch["in_store_stock"] == 0])
            na_b = len(df_batch[df_batch["in_store_stock"] < 0])

            bc1, bc2, bc3, bc4 = st.columns(4)
            bc1.metric("Stores Checked", len(df_batch))
            bc2.metric("In Stock", in_stock_b)
            bc3.metric("Out of Stock", out_b)
            bc4.metric("Not Available", na_b)

    st.divider()

    # Latest stock across all stores (cached)
    st.subheader("Latest Stock Across All Stores")
    if latest_stock:
        rows = []
        for sid, data in sorted(latest_stock.items(), key=lambda x: x[1].get("in_store_stock", -1), reverse=True):
            stock = data.get("in_store_stock", -1)
            if stock >= 0:
                rows.append({
                    "Store": data.get("store_name", f"Store {sid}"),
                    "Type": data.get("store_type", ""),
                    "In-Store Stock": stock,
                    "SAP Stock": data.get("sap_stock", 0),
                    "Price": f"${float(data.get('price', 0)):.2f}" if data.get("price") else "-",
                    "Last Checked On": format_timestamp_for_timezone(data.get("timestamp", ""), _display_tz),
                })
        if rows:
            df_latest = pd.DataFrame(rows)
            st.dataframe(
                df_latest,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "In-Store Stock": st.column_config.NumberColumn(format="%d units"),
                    "SAP Stock": st.column_config.NumberColumn(format="%d units"),
                }
            )

            st.subheader("Stock Levels by Store")
            chart_df = df_latest[["Store", "In-Store Stock"]].set_index("Store")
            st.bar_chart(chart_df, use_container_width=True)
    else:
        st.info("ℹ️ No store stock history yet. Run a stock check first.")

# --- Tab 3: Total Inventory --------------------------------------------
with tab_inventory:
    st.header("Total Inventory Over Time")
    st.caption(selected_product_name)
    st.caption("Tracks the combined inventory across all FairPrice stores for each stock check batch.")
    st.caption("Sales inference is conservative: stock drops count as minimum units sold, while stock increases count as minimum restocks.")

    if not inventory_totals_df.empty:
        df_totals = inventory_totals_df.copy()
        df_totals["timestamp"] = convert_timestamp_series(df_totals["timestamp"], _display_tz)

        # Current totals at the top
        if not df_totals.empty:
            latest_row = df_totals.iloc[-1]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Units (All Stores)", f"{int(latest_row['total_units']):,}")
            c2.metric("Stores In Stock", int(latest_row["stores_in_stock"]))
            c3.metric("Stores Out Of Stock", int(latest_row["stores_out_of_stock"]))
            c4.metric("Avg Stock / Store", f"{latest_row['avg_stock_per_store']:.1f}")

            # Add warehouse stock for context
            wh = cached_get_latest_warehouse(selected_product_sku)
            if wh:
                combined = int(latest_row["total_units"]) + int(wh["in_store_stock"])
                st.info(f"ℹ️ **Combined Inventory (Stores + Warehouse):** {combined:,} units "
                        f"({int(latest_row['total_units']):,}) in stores + {int(wh['in_store_stock']):,} in warehouse")

            st.divider()

            m1, m2, m3 = st.columns(3)
            m1.metric("Min Sold Since Prev Batch", int(latest_row["min_units_sold"]))
            m2.metric("Min Restocked Since Prev Batch", int(latest_row["min_units_restocked"]))
            m3.metric("Net Inventory Change", int(latest_row["net_change"]))

            st.divider()

            # Charts
            st.subheader("Total Inventory Over Time")
            st.line_chart(df_totals.set_index("timestamp")[["total_units"]], use_container_width=True)

            st.subheader("Inferred Movement Per Batch")
            st.line_chart(
                df_totals.set_index("timestamp")[["min_units_sold", "min_units_restocked"]],
                use_container_width=True,
            )

            cumulative_sales_df = df_totals[["timestamp", "min_units_sold", "min_units_restocked"]].copy()
            cumulative_sales_df["cumulative_min_sold"] = cumulative_sales_df["min_units_sold"].cumsum()
            cumulative_sales_df["cumulative_min_restocked"] = cumulative_sales_df["min_units_restocked"].cumsum()

            st.subheader("Total Inferred Sales Across Singapore")
            total_min_sold_all = int(df_totals["min_units_sold"].sum())
            total_min_restocked_all = int(df_totals["min_units_restocked"].sum())
            total_net_change_all = int(df_totals["net_change"].sum())
            s1, s2, s3 = st.columns(3)
            s1.metric("Cumulative Min Sold", f"{total_min_sold_all:,}")
            s2.metric("Cumulative Min Restocked", f"{total_min_restocked_all:,}")
            s3.metric("Cumulative Net Change", f"{total_net_change_all:,}")

            st.line_chart(
                cumulative_sales_df.set_index("timestamp")[["cumulative_min_sold"]],
                use_container_width=True,
            )

            st.subheader("Stores In Stock Over Time")
            st.line_chart(df_totals.set_index("timestamp")[["stores_in_stock", "stores_out_of_stock"]], use_container_width=True)

            st.subheader("Average Stock Per Store Over Time")
            st.line_chart(df_totals.set_index("timestamp")[["avg_stock_per_store"]], use_container_width=True)

            st.divider()

            if not store_movement_summary.empty:
                st.subheader("Store-Level Sales Inference")
                plot_df = (
                    store_movement_summary[["store_name", "min_units_sold"]]
                    .sort_values("min_units_sold", ascending=False)
                    .head(30)
                    .set_index("store_name")
                )
                st.bar_chart(plot_df, use_container_width=True)

                display_movement_df = store_movement_summary[[
                    "store_name", "store_type", "latest_stock", "min_units_sold",
                    "min_units_restocked", "net_change", "observations", "first_seen", "last_seen"
                ]].copy()
                display_movement_df.columns = [
                    "Store", "Type", "Latest Stock", "Min Sold Observed",
                    "Min Restocked", "Net Change", "Batches Seen", "First Seen", "Last Seen"
                ]
                st.dataframe(
                    display_movement_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Latest Stock": st.column_config.NumberColumn(format="%d units"),
                        "Min Sold Observed": st.column_config.NumberColumn(format="%d units"),
                        "Min Restocked": st.column_config.NumberColumn(format="%d units"),
                        "Net Change": st.column_config.NumberColumn(format="%+d units"),
                    }
                )

            # Raw data table
            st.subheader("Batch Summary Table")
            display_df = df_totals[[
                "timestamp", "batch_id", "total_units", "min_units_sold",
                "min_units_restocked", "net_change", "stores_checked",
                "stores_with_product", "stores_in_stock", "stores_out_of_stock",
                "avg_stock_per_store"
            ]].copy()
            display_df.columns = [
                "Timestamp", "Batch ID", "Total Units", "Min Sold",
                "Min Restocked", "Net Change", "Stores Checked",
                "Stores With Product", "In Stock", "Out of Stock", "Avg Stock/Store"
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Total Units": st.column_config.NumberColumn(format="%d units"),
                    "Min Sold": st.column_config.NumberColumn(format="%d units"),
                    "Min Restocked": st.column_config.NumberColumn(format="%d units"),
                    "Net Change": st.column_config.NumberColumn(format="%+d units"),
                }
            )

            # Download button
            csv_data = df_totals.to_csv(index=False)
            st.download_button(
                "Download Total Inventory History",
                csv_data,
                file_name="total_inventory_history.csv",
                mime="text/csv",
                use_container_width=True,
            )
    else:
        st.info("ℹ️ No store stock history yet. Run a stock check first to start tracking total inventory.")
