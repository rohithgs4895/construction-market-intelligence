"""
fetch_competitors.py
--------------------
Queries OpenStreetMap via the Overpass API (overpy) to find construction-related
businesses near the 15 target metros.

Fetches:
  - Hardware stores (shop=hardware)
  - Building material suppliers (shop=building_materials)
  - Construction companies (office=construction / craft=construction)
  - Lumber yards (shop=lumber)
  - Plumbing/HVAC suppliers (shop=plumbing / shop=hvac)

Output: data/raw/competitors.csv with name, category, lat, lon, city, state
"""

import time
import overpy
import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = RAW_DIR / "competitors.csv"

# Bounding boxes for the 15 metro areas [south, west, north, east]
MARKETS = [
    {"city": "Phoenix",      "state": "AZ", "bbox": (33.05, -113.00, 33.95, -111.60)},
    {"city": "Dallas",       "state": "TX", "bbox": (32.55, -97.55, 33.25, -96.45)},
    {"city": "Houston",      "state": "TX", "bbox": (29.40, -95.80, 30.20, -94.80)},
    {"city": "Austin",       "state": "TX", "bbox": (29.95, -98.10, 30.65, -97.40)},
    {"city": "Atlanta",      "state": "GA", "bbox": (33.45, -84.90, 34.10, -83.90)},
    {"city": "Charlotte",    "state": "NC", "bbox": (34.90, -81.10, 35.45, -80.50)},
    {"city": "Nashville",    "state": "TN", "bbox": (35.95, -87.10, 36.45, -86.50)},
    {"city": "Denver",       "state": "CO", "bbox": (39.50, -105.20, 39.95, -104.60)},
    {"city": "Las Vegas",    "state": "NV", "bbox": (35.95, -115.40, 36.40, -114.90)},
    {"city": "Orlando",      "state": "FL", "bbox": (28.30, -81.65, 28.75, -81.05)},
    {"city": "Tampa",        "state": "FL", "bbox": (27.70, -82.75, 28.20, -82.20)},
    {"city": "Jacksonville", "state": "FL", "bbox": (30.00, -82.00, 30.60, -81.30)},
    {"city": "Raleigh",      "state": "NC", "bbox": (35.60, -79.00, 36.10, -78.40)},
    {"city": "San Antonio",  "state": "TX", "bbox": (29.20, -98.75, 29.70, -98.15)},
    {"city": "Fort Worth",   "state": "TX", "bbox": (32.50, -97.55, 32.95, -97.00)},
]

# OSM tags for construction-related businesses
QUERIES = [
    ("hardware_store",        'node["shop"="hardware"]'),
    ("hardware_store",        'way["shop"="hardware"]'),
    ("building_materials",    'node["shop"="building_materials"]'),
    ("building_materials",    'way["shop"="building_materials"]'),
    ("lumber_yard",           'node["shop"="lumber"]'),
    ("lumber_yard",           'way["shop"="lumber"]'),
    ("construction_company",  'node["office"="construction"]'),
    ("construction_company",  'node["craft"="construction"]'),
    ("plumbing_supplier",     'node["shop"="plumbing"]'),
    ("hvac_supplier",         'node["shop"="hvac"]'),
    ("home_improvement",      'node["shop"="doityourself"]'),
]


def build_overpass_query(bbox: tuple, queries: list) -> str:
    """Build an Overpass QL query for a bounding box."""
    s, w, n, e = bbox
    bbox_str = f"{s},{w},{n},{e}"
    parts = []
    for _, tag_query in queries:
        parts.append(f"{tag_query}({bbox_str});")
    return f"[out:json][timeout:30];\n(\n  " + "\n  ".join(parts) + "\n);\nout center;"


def extract_records(result, city: str, state: str, category_map: dict) -> list:
    """Extract node/way records from overpy result."""
    records = []
    for node in result.nodes:
        name = node.tags.get("name", "")
        shop = node.tags.get("shop", "")
        office = node.tags.get("office", "")
        craft = node.tags.get("craft", "")

        category = (
            category_map.get(shop) or
            category_map.get(office) or
            category_map.get(craft) or
            "other"
        )

        records.append({
            "osm_id": node.id,
            "osm_type": "node",
            "name": name,
            "category": category,
            "shop_tag": shop or office or craft,
            "lat": float(node.lat),
            "lon": float(node.lon),
            "city": city,
            "state": state,
            "website": node.tags.get("website", ""),
            "phone": node.tags.get("phone", ""),
            "brand": node.tags.get("brand", ""),
        })

    for way in result.ways:
        name = way.tags.get("name", "")
        shop = way.tags.get("shop", "")
        office = way.tags.get("office", "")
        craft = way.tags.get("craft", "")
        category = (
            category_map.get(shop) or
            category_map.get(office) or
            category_map.get(craft) or
            "other"
        )
        # Ways have a center attribute when using "out center"
        if hasattr(way, "center_lat") and way.center_lat:
            lat, lon = float(way.center_lat), float(way.center_lon)
        else:
            continue

        records.append({
            "osm_id": way.id,
            "osm_type": "way",
            "name": name,
            "category": category,
            "shop_tag": shop or office or craft,
            "lat": lat,
            "lon": lon,
            "city": city,
            "state": state,
            "website": way.tags.get("website", ""),
            "phone": way.tags.get("phone", ""),
            "brand": way.tags.get("brand", ""),
        })

    return records


CATEGORY_MAP = {
    "hardware":           "hardware_store",
    "building_materials": "building_materials",
    "lumber":             "lumber_yard",
    "construction":       "construction_company",
    "plumbing":           "plumbing_supplier",
    "hvac":               "hvac_supplier",
    "doityourself":       "home_improvement",
}


def main():
    print("=" * 60)
    print("OpenStreetMap Competitor Fetch — Top 15 Construction Markets")
    print("=" * 60)

    api = overpy.Overpass()
    all_records = []

    for market in MARKETS:
        city, state, bbox = market["city"], market["state"], market["bbox"]
        print(f"  Querying {city}, {state}...", end=" ", flush=True)

        query = build_overpass_query(bbox, QUERIES)

        try:
            result = api.query(query)
            records = extract_records(result, city, state, CATEGORY_MAP)
            all_records.extend(records)
            print(f"{len(records)} locations found")
        except overpy.exception.OverPyException as e:
            print(f"Overpass error: {e}")
        except Exception as e:
            print(f"Error: {e}")

        # Respect Overpass rate limits
        time.sleep(15)

    df = pd.DataFrame(all_records)

    if df.empty:
        print("\nNo OSM data retrieved. Creating placeholder file.")
        df = pd.DataFrame(columns=[
            "osm_id","osm_type","name","category","shop_tag",
            "lat","lon","city","state","website","phone","brand"
        ])
    else:
        # Remove duplicates (same OSM ID)
        df = df.drop_duplicates(subset=["osm_id", "osm_type"])

        # Filter out unnamed locations
        named = df[df["name"].str.len() > 0]
        print(f"\nTotal locations: {len(df)} ({len(named)} named)")

        # Category summary
        print("\nBy category:")
        print(df.groupby(["city", "category"]).size().unstack(fill_value=0).to_string())

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved {len(df)} competitor locations to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
