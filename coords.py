import requests, json, time

# Postal codes spread across all regions of Singapore
postal_codes = [
    "520101", "530201", "540301", "550401", "560501",
    "570601", "580701", "590801", "600901", "610001",
    "620101", "630201", "640301", "650401", "660501",
    "670601", "680701", "690801", "700901", "710001",
    "720101", "730201", "738001", "750001", "760001",
    "770001", "780001", "790001", "800001", "810001",
    "820001", "828001", "829001",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

stores = {}
for pc in postal_codes:
    url = f"https://public-api.omni.fairprice.com.sg/address/search?isBoysBrigade=false&service=cac&term={pc}&type=all"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        for s in r.json().get("data", {}).get("fpstores", []):
            sid = str(s["id"])
            if sid not in stores and s.get("lat") and s.get("long"):
                stores[sid] = {
                    "id": sid,
                    "name": s["name"],
                    "address": s.get("address") or s.get("address1", ""),
                    "lat": s["lat"],
                    "lng": s["long"],  # API returns "long", we map to "lng"
                    "postalCode": s.get("postalCode", ""),
                    "storeType": s.get("storeType", ""),
                }
        print(f"Postal {pc}: found {len(stores)} unique stores so far")
    except Exception as e:
        print(f"Error for {pc}: {e}")
    time.sleep(0.3)  # Be polite, avoid rate limiting

with open("stores_with_coords.json", "w") as f:
    json.dump(list(stores.values()), f, indent=2)

print(f"\nDone! Saved {len(stores)} stores to stores_with_coords.json")
