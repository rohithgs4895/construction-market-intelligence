"""
create_feature_layers.py
------------------------
Loads processed data from SQLite and exports shapefiles for ArcGIS Online upload.

Outputs (in outputs/):
  - market_boundaries.shp   : 15 metro area centroids with all KPIs
  - permit_hotspots.shp     : markets symbolized by permit volume
  - competitor_locations.shp: individual OSM competitor points
  - market_summary.shp      : full market_summary view as points

Requirements: geopandas, shapely, fiona
"""

import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

PROC_DIR   = Path(__file__).parent.parent / "data" / "processed"
OUTPUTS    = Path(__file__).parent.parent / "outputs"
OUTPUTS.mkdir(parents=True, exist_ok=True)
DB_PATH    = PROC_DIR / "market_intelligence.db"

CRS_WGS84  = "EPSG:4326"   # Standard lat/lon for ArcGIS Online


def load_table(conn: sqlite3.Connection, query: str) -> pd.DataFrame:
    return pd.read_sql(query, conn)


def make_point_gdf(df: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> gpd.GeoDataFrame:
    """Convert a DataFrame with lat/lon columns into a GeoDataFrame of Points."""
    geometry = [Point(row[lon_col], row[lat_col]) for _, row in df.iterrows()]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=CRS_WGS84)
    return gdf


def truncate_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Shapefiles limit field names to 10 characters.
    Rename columns that are too long.
    """
    rename_map = {
        "permit_total_units":        "permit_tot",
        "single_family_units":       "sf_units",
        "multifamily_units":         "mf_units",
        "pct_single_family":         "pct_sf",
        "permit_rank":               "perm_rank",
        "median_household_income":   "med_income",
        "total_housing_units":       "tot_housing",
        "homeownership_rate_pct":    "own_rate",
        "median_home_value":         "med_hvalue",
        "total_competitors":         "tot_comp",
        "hardware_stores":           "hdwr_st",
        "building_material_stores":  "bldg_mat",
        "permits_per_1k_pop":        "prmt_1k",
        "competitors_per_100k":      "comp_100k",
        "opportunity_score":         "opp_score",
        "permit_year":               "perm_year",
        "cbsa_code":                 "cbsa",
        "market_id":                 "mkt_id",
        "pop_age_25_44":             "pop_25_44",
        "pct_prime_homebuying_age":  "pct_prime",
        "data_source":               "data_src",
        "acs_year":                  "acs_year",
    }
    cols_to_rename = {k: v for k, v in rename_map.items() if k in gdf.columns}
    return gdf.rename(columns=cols_to_rename)


def export_market_boundaries(conn: sqlite3.Connection):
    """Export 15 metro centroids with full KPI attributes."""
    print("  Exporting market_boundaries.shp...")
    df = load_table(conn, "SELECT * FROM market_summary")
    gdf = make_point_gdf(df)
    gdf = truncate_columns(gdf)
    out = OUTPUTS / "market_boundaries.shp"
    gdf.to_file(out)
    print(f"    Saved {len(gdf)} markets -> {out}")
    return gdf


def export_permit_hotspots(conn: sqlite3.Connection):
    """Export permit data with hotspot classification."""
    print("  Exporting permit_hotspots.shp...")
    df = load_table(conn, """
        SELECT
            ms.city, ms.state, ms.lat, ms.lon,
            ms.permit_total_units,
            ms.single_family_units,
            ms.multifamily_units,
            ms.pct_single_family,
            ms.permits_per_1k_pop,
            ms.permit_rank,
            CASE
                WHEN ms.permit_total_units >= 40000 THEN 'Tier 1 - Very High'
                WHEN ms.permit_total_units >= 25000 THEN 'Tier 2 - High'
                WHEN ms.permit_total_units >= 18000 THEN 'Tier 3 - Medium'
                ELSE 'Tier 4 - Emerging'
            END AS hotspot_tier
        FROM market_summary ms
        ORDER BY permit_rank
    """)
    gdf = make_point_gdf(df)
    gdf = truncate_columns(gdf)
    out = OUTPUTS / "permit_hotspots.shp"
    gdf.to_file(out)
    print(f"    Saved {len(gdf)} hotspots -> {out}")


def export_competitor_locations(conn: sqlite3.Connection):
    """Export individual competitor point locations."""
    print("  Exporting competitor_locations.shp...")
    df = load_table(conn, """
        SELECT
            osm_id, name, category, shop_tag,
            lat, lon, city, state,
            brand, phone, website
        FROM competitors
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND lat BETWEEN 20 AND 50
          AND lon BETWEEN -130 AND -60
    """)

    if df.empty:
        print("    No competitor data — skipping shapefile")
        return

    gdf = make_point_gdf(df)
    gdf = truncate_columns(gdf)
    out = OUTPUTS / "competitor_locations.shp"
    gdf.to_file(out)
    print(f"    Saved {len(gdf)} competitors -> {out}")


def export_market_summary(conn: sqlite3.Connection):
    """Export the full market_summary view with opportunity scores."""
    print("  Exporting market_summary.shp...")
    df = load_table(conn, """
        SELECT
            city, state, lat, lon,
            population,
            median_household_income,
            total_competitors,
            permit_total_units,
            permits_per_1k_pop,
            competitors_per_100k,
            opportunity_score,
            homeownership_rate_pct,
            median_home_value
        FROM market_summary
        ORDER BY opportunity_score DESC
    """)
    gdf = make_point_gdf(df)
    gdf = truncate_columns(gdf)
    out = OUTPUTS / "market_summary.shp"
    gdf.to_file(out)
    print(f"    Saved {len(gdf)} market summary points -> {out}")


def print_arcgis_instructions():
    print("""
ArcGIS Online Upload Instructions
===================================
1. Sign in to ArcGIS Online (arcgis.com)
2. Go to Content > My Content > Add Item > From your computer
3. Upload each .zip of the shapefile components:
     - market_boundaries.zip  (contains .shp, .dbf, .shx, .prj)
     - permit_hotspots.zip
     - competitor_locations.zip
     - market_summary.zip
4. Publish each as a Hosted Feature Layer
5. In ArcGIS Dashboards:
     - Use market_summary layer for the main map
     - Configure indicators for: top market by permits, avg income, total competitors
     - Add serial chart: permits by city
     - Add list widget: top_markets ranked by opportunity_score
""")


def main():
    print("=" * 60)
    print("Creating ArcGIS Feature Layer Shapefiles")
    print("=" * 60)

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DB_PATH}\n"
            "Run build_database.py first."
        )

    conn = sqlite3.connect(DB_PATH)

    try:
        export_market_boundaries(conn)
        export_permit_hotspots(conn)
        export_competitor_locations(conn)
        export_market_summary(conn)
    finally:
        conn.close()

    # List output files
    print(f"\nOutput files in {OUTPUTS}:")
    for f in sorted(OUTPUTS.iterdir()):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name:40s}  {size_kb:6.1f} KB")

    print_arcgis_instructions()


if __name__ == "__main__":
    main()
