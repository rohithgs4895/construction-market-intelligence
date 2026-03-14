"""
publish_to_arcgis.py
--------------------
Zips shapefiles in outputs/ and publishes each as a hosted feature layer
on ArcGIS Online using the ArcGIS API for Python.

Layers published:
  market_boundaries   → "CMI_Market_Boundaries"
  permit_hotspots     → "CMI_Permit_Hotspots"
  competitor_locations→ "CMI_Competitor_Locations"
  market_summary      → "CMI_Market_Summary"

Usage:
  python src/publish_to_arcgis.py

  Prompts for ArcGIS Online username and password interactively.
  Set ARCGIS_USERNAME / ARCGIS_PASSWORD in .env to skip the prompt.

Requirements:
  pip install arcgis python-dotenv
"""

import os
import sys
import zipfile
import getpass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

OUTPUTS = Path(__file__).parent.parent / "outputs"

# Shapefile base names → ArcGIS Online item title
LAYERS = [
    {
        "shapefile": "market_boundaries",
        "title":     "CMI_Market_Boundaries",
        "tags":      "construction, market intelligence, permits, demographics",
        "description": (
            "Top 15 US construction market centroids with building permit counts, "
            "demographic KPIs, and opportunity scores. Source: Census BPS + ACS 2022."
        ),
        "snippet":   "Construction market KPIs for 15 high-growth US metros.",
    },
    {
        "shapefile": "permit_hotspots",
        "title":     "CMI_Permit_Hotspots",
        "tags":      "construction, building permits, hotspots, Census",
        "description": (
            "15 metro areas tiered by annual building permit volume (2023). "
            "Tier 1 = 40k+ units/yr, Tier 4 = emerging markets. Source: Census BPS 2023."
        ),
        "snippet":   "Building permit volume tiers for top 15 US construction markets.",
    },
    {
        "shapefile": "competitor_locations",
        "title":     "CMI_Competitor_Locations",
        "tags":      "construction, competitors, hardware, OpenStreetMap",
        "description": (
            "Hardware stores, building material suppliers, lumber yards, and construction "
            "companies across the top 15 US construction markets. Source: OpenStreetMap via Overpass API."
        ),
        "snippet":   "597 construction-related business locations from OpenStreetMap.",
    },
    {
        "shapefile": "market_summary",
        "title":     "CMI_Market_Summary",
        "tags":      "construction, market intelligence, opportunity score, dashboard",
        "description": (
            "Full market summary with opportunity scores, permit data, demographics, "
            "and competitor density. Use this layer as the primary map layer in ArcGIS Dashboards."
        ),
        "snippet":   "Market opportunity scores and KPIs for dashboard map layer.",
    },
]

# Required shapefile extensions to include in the zip
SHP_EXTENSIONS = [".shp", ".dbf", ".shx", ".prj", ".cpg"]


def zip_shapefile(base_name: str) -> Path:
    """
    Zip all components of a shapefile into outputs/<base_name>.zip.
    Returns the path to the zip file.
    """
    zip_path = OUTPUTS / f"{base_name}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for ext in SHP_EXTENSIONS:
            src = OUTPUTS / f"{base_name}{ext}"
            if src.exists():
                zf.write(src, arcname=f"{base_name}{ext}")

        # List what ended up in the zip
        names = zf.namelist()

    size_kb = zip_path.stat().st_size / 1024
    print(f"    Zipped: {zip_path.name}  ({size_kb:.1f} KB)  [{', '.join(names)}]")
    return zip_path


def delete_existing_item(gis, title: str):
    """Search for and delete any existing item with the same title to allow re-publish."""
    results = gis.content.search(query=f'title:"{title}"', item_type="Feature Layer")
    for item in results:
        if item.title == title:
            print(f"    Deleting existing item: {item.id}")
            item.delete()


def publish_layer(gis, layer_def: dict) -> dict:
    """
    Upload a zipped shapefile to ArcGIS Online and publish it as a hosted feature layer.
    Returns a dict with item_id and url.
    """
    base   = layer_def["shapefile"]
    title  = layer_def["title"]

    print(f"\n  Publishing: {title}")
    print(f"    Source shapefile: {base}.shp")

    # Step 1: Zip
    zip_path = zip_shapefile(base)

    # Step 2: Remove any pre-existing item with this title
    delete_existing_item(gis, title)

    # Step 3: Upload zip as a Shapefile item
    print(f"    Uploading {zip_path.name} to ArcGIS Online...", end=" ", flush=True)
    item_props = {
        "title":       title,
        "type":        "Shapefile",
        "tags":        layer_def["tags"],
        "description": layer_def["description"],
        "snippet":     layer_def["snippet"],
    }
    shp_item = gis.content.add(item_properties=item_props, data=str(zip_path))
    print(f"uploaded (item id: {shp_item.id})")

    # Step 4: Publish as hosted feature layer
    print(f"    Publishing as hosted feature layer...", end=" ", flush=True)
    publish_params = {
        "name":           title,
        "layerInfo":      {"capabilities": "Query,Extract"},
        "hasStaticData":  False,
    }
    fl_item = shp_item.publish(publish_parameters=publish_params, overwrite=True)
    print(f"done")

    # Step 5: Share publicly (optional — comment out if org-only is preferred)
    fl_item.share(everyone=True)

    # Build the feature service URL
    service_url = fl_item.url
    portal_url  = f"https://www.arcgis.com/home/item.html?id={fl_item.id}"

    print(f"    Item ID  : {fl_item.id}")
    print(f"    Layer URL: {service_url}")
    print(f"    Portal   : {portal_url}")

    return {
        "title":       title,
        "shapefile":   base,
        "item_id":     fl_item.id,
        "service_url": service_url,
        "portal_url":  portal_url,
    }


def print_summary(results: list):
    print("\n" + "=" * 70)
    print("PUBLISH COMPLETE — Feature Layer Summary")
    print("=" * 70)
    for r in results:
        print(f"\n  {r['title']}")
        print(f"    Item ID : {r['item_id']}")
        print(f"    REST URL: {r['service_url']}")
        print(f"    Portal  : {r['portal_url']}")

    print("\n" + "-" * 70)
    print("Next steps — ArcGIS Dashboard:")
    print("  1. Open ArcGIS Online → Content and confirm all 4 items are published")
    print("  2. Create a new Dashboard (arcgis.com → Apps → Dashboards)")
    print("  3. Add map widget using CMI_Market_Summary as the primary layer")
    print("  4. Symbolize by 'opp_score' (opportunity score) with graduated symbols")
    print("  5. Add indicator widgets: top market, avg income, total competitors")
    print("  6. Add serial chart widget: permit_tot by city (from CMI_Permit_Hotspots)")
    print("  7. Add list widget: cities ranked by opp_score")
    print("-" * 70)


def get_credentials() -> tuple[str, str]:
    """Get ArcGIS Online credentials from .env or interactive prompt."""
    username = os.getenv("ARCGIS_USERNAME", "").strip()
    password = os.getenv("ARCGIS_PASSWORD", "").strip()

    if not username:
        username = input("ArcGIS Online username: ").strip()
    else:
        print(f"ArcGIS username (from .env): {username}")

    if not password:
        password = getpass.getpass("ArcGIS Online password: ")

    return username, password


def main():
    print("=" * 70)
    print("Construction Market Intelligence — ArcGIS Online Publisher")
    print("=" * 70)

    # Verify outputs exist
    missing = []
    for layer in LAYERS:
        shp = OUTPUTS / f"{layer['shapefile']}.shp"
        if not shp.exists():
            missing.append(str(shp))
    if missing:
        print("ERROR: Missing shapefiles:")
        for m in missing:
            print(f"  {m}")
        print("Run create_feature_layers.py first.")
        sys.exit(1)

    # Connect to ArcGIS Online
    try:
        from arcgis.gis import GIS
    except ImportError:
        print("ERROR: arcgis package not installed. Run: pip install arcgis")
        sys.exit(1)

    username, password = get_credentials()

    print(f"\nConnecting to ArcGIS Online as '{username}'...")
    try:
        gis = GIS("https://www.arcgis.com", username, password)
        print(f"Connected: {gis.properties.user.fullName} ({gis.properties.user.username})")
        print(f"Org      : {gis.properties.name}")
    except Exception as e:
        print(f"ERROR: Could not connect to ArcGIS Online: {e}")
        sys.exit(1)

    # Publish each layer
    results = []
    for layer_def in LAYERS:
        try:
            result = publish_layer(gis, layer_def)
            results.append(result)
        except Exception as e:
            print(f"\n  ERROR publishing {layer_def['title']}: {e}")
            results.append({
                "title":       layer_def["title"],
                "shapefile":   layer_def["shapefile"],
                "item_id":     "FAILED",
                "service_url": str(e),
                "portal_url":  "",
            })

    print_summary(results)

    # Save item IDs to arcgis/published_layers.txt for reference
    ref_file = Path(__file__).parent.parent / "arcgis" / "published_layers.txt"
    ref_file.parent.mkdir(exist_ok=True)
    with open(ref_file, "w") as f:
        f.write("Construction Market Intelligence — Published Feature Layers\n")
        f.write("=" * 60 + "\n\n")
        for r in results:
            f.write(f"Title    : {r['title']}\n")
            f.write(f"Item ID  : {r['item_id']}\n")
            f.write(f"REST URL : {r['service_url']}\n")
            f.write(f"Portal   : {r['portal_url']}\n\n")
    print(f"\nItem IDs saved to: {ref_file}")


if __name__ == "__main__":
    main()
