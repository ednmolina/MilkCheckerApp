"""
FairPrice API integration module.
Uses the product/v2 API with per-store storeId to get real in-store stock counts.

Optimizations:
- requests.Session() for TCP connection reuse
- Cached delivery price (fetched once, reused across calls)
- Single API call per store (INSTORE only; price from cache)
"""

import requests
import time
from typing import Optional

PRODUCT_SKU = "13282308"
PRODUCT_SLUG = "meiji-low-fat-high-protein-milk-original-350ml-13282308"
WAREHOUSE_STORE_ID = "165"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Reusable session for TCP connection pooling
_session: Optional[requests.Session] = None

# Cached delivery price (rarely changes, fetched once per job run)
_cached_price: Optional[dict] = None
_price_fetched_at: float = 0
PRICE_CACHE_TTL = 3600  # 1 hour

def _get_session() -> requests.Session:
    """Get or create a reusable requests session with connection pooling."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
        # Connection pooling: reuse up to 20 connections
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10, pool_maxsize=20, max_retries=2
        )
        _session.mount("https://", adapter)
    return _session

def _fetch_delivery_price() -> dict:
    """
    Fetch the offer/sale price from the DELIVERY API.
    This is cached because the price rarely changes (maybe once a week).
    Returns {"price": float, "mrp": float, "discount": float}.
    """
    global _cached_price, _price_fetched_at

    now = time.time()
    if _cached_price and (now - _price_fetched_at) < PRICE_CACHE_TTL:
        return _cached_price

    session = _get_session()
    url = (
        f"https://website-api.omni.fairprice.com.sg/api/product/v2"
        f"?storeId={WAREHOUSE_STORE_ID}&sku={PRODUCT_SKU}"
        f"&pageType=product-listing&orderType=DELIVERY"
    )
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        products = resp.json().get("data", {}).get("product", [])
        if products:
            p = products[0]
            offers = p.get("offers") or []
            ssd = (p.get("storeSpecificData") or [{}])
            ssd = ssd[0] if ssd else {}
            mrp = float(ssd.get("mrp", 0) or 0)
            offer_price = float(offers[0].get("price", mrp)) if offers else mrp
            discount = round(offer_price - mrp, 2) if offer_price and mrp else 0

            _cached_price = {"price": offer_price, "mrp": mrp, "discount": discount}
            _price_fetched_at = now
            return _cached_price
    except Exception:
        pass

    # Fallback if fetch fails
    if _cached_price:
        return _cached_price
    return {"price": 0, "mrp": 0, "discount": 0}

def search_by_postal_code(postal_code: str) -> dict:
    """
    Search for addresses and nearby FairPrice stores by postal code.
    Returns {"addresses": [...], "stores": [...]}.
    Uses 1 API call.
    """
    session = _get_session()
    url = (
        f"https://public-api.omni.fairprice.com.sg/address/search"
        f"?isBoysBrigade=false&service=cac&term={postal_code}&type=all"
    )
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json().get("data", {})

    addresses = []
    for a in data.get("addresses", []):
        addresses.append({
            "building_name": a.get("buildingName", ""),
            "building_number": a.get("buildingNumber", ""),
            "street_name": a.get("streetName", ""),
            "postcode": a.get("postcode", ""),
            "lat": a.get("lat"),
            "long": a.get("long"),
        })

    stores = []
    for s in data.get("fpstores", []):
        stores.append({
            "id": str(s.get("id", "")),
            "identifier": str(s.get("identifier", "")),
            "name": s.get("name", ""),
            "address": s.get("address") or s.get("address1", ""),
            "store_type": s.get("storeType", ""),
            "lat": s.get("lat"),
            "long": s.get("long"),
            "phone": s.get("phone", ""),
            "is_search_browse_enabled": bool(s.get("isSearchAndBrowseEnabled")),
            "zone_id": s.get("zoneId", ""),
        })

    return {"addresses": addresses, "stores": stores}

def get_warehouse_stock() -> Optional[dict]:
    """
    Fetch warehouse stock. Uses 1 API call for stock (INSTORE)
    + reuses cached price from DELIVERY (fetched once per hour).
    Total: 1 API call (or 2 if price cache expired).
    """
    session = _get_session()

    # 1. Get stock data from INSTORE API (1 call)
    url_instore = (
        f"https://website-api.omni.fairprice.com.sg/api/product/v2"
        f"?storeId={WAREHOUSE_STORE_ID}&sku={PRODUCT_SKU}"
        f"&pageType=product-listing&orderType=INSTORE"
    )
    resp = session.get(url_instore, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    products = data.get("data", {}).get("product", [])
    if not products:
        return None

    p = products[0]
    ssd = p.get("storeSpecificData", [{}])
    ssd = ssd[0] if ssd else {}

    # 2. Get price from cache (0 calls if cached, 1 call if expired)
    price_data = _fetch_delivery_price()

    return {
        "product_name": p.get("name", ""),
        "in_store_stock": int(ssd.get("inStoreStock", 0) or 0),
        "online_stock": int(ssd.get("onlineStock", 0) or 0),
        "sap_stock": int(ssd.get("sapStock", 0) or 0),
        "price": price_data["price"],
        "mrp": price_data["mrp"],
        "discount": price_data["discount"],
    }

def get_store_stock(store_id: str) -> Optional[dict]:
    """
    Get real in-store stock for a specific store.
    Uses 1 API call (INSTORE only). Price comes from cache.
    """
    session = _get_session()
    url = (
        f"https://website-api.omni.fairprice.com.sg/api/product/v2"
        f"?storeId={store_id}&sku={PRODUCT_SKU}"
        f"&pageType=product-listing&orderType=INSTORE"
    )
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        products = data.get("data", {}).get("product", [])
        if not products:
            return None

        p = products[0]
        ssd = p.get("storeSpecificData", [{}])
        ssd = ssd[0] if ssd else {}

        # Use cached price instead of making another API call
        price_data = _fetch_delivery_price()

        return {
            "store_id": store_id,
            "in_store_stock": int(ssd.get("inStoreStock", 0) or 0),
            "sap_stock": int(ssd.get("sapStock", 0) or 0),
            "price": price_data["price"],
            "mrp": price_data["mrp"],
        }
    except Exception:
        return None
