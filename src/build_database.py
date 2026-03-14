"""
build_database.py
-----------------
Loads all raw CSVs into a SQLite database and creates analytical views.

Database: data/processed/market_intelligence.db

Tables:
  - markets         : master list of the 15 metros with lat/lon centroid
  - permits         : annual building permit counts
  - demographics    : ACS population/income/housing data
  - competitors     : OSM competitor locations

Views:
  - market_summary     : one row per market with all KPIs joined
  - top_markets        : ranked by opportunity score
  - competitor_density : competitor count per 100k population
"""

import sqlite3
import pandas as pd
from pathlib import Path

RAW_DIR  = Path(__file__).parent.parent / "data" / "raw"
PROC_DIR = Path(__file__).parent.parent / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH  = PROC_DIR / "market_intelligence.db"

# Market centroids (lat/lon) for spatial reference
MARKET_CENTROIDS = {
    "Phoenix":      (33.4484,  -112.0740),
    "Dallas":       (32.7767,  -96.7970),
    "Houston":      (29.7604,  -95.3698),
    "Austin":       (30.2672,  -97.7431),
    "Atlanta":      (33.7490,  -84.3880),
    "Charlotte":    (35.2271,  -80.8431),
    "Nashville":    (36.1627,  -86.7816),
    "Denver":       (39.7392,  -104.9903),
    "Las Vegas":    (36.1699,  -115.1398),
    "Orlando":      (28.5383,  -81.3792),
    "Tampa":        (27.9506,  -82.4572),
    "Jacksonville": (30.3322,  -81.6557),
    "Raleigh":      (35.7796,  -78.6382),
    "San Antonio":  (29.4241,  -98.4936),
    "Fort Worth":   (32.7555,  -97.3308),
}


def load_raw_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all three raw CSVs."""
    permits_file = RAW_DIR / "building_permits.csv"
    demo_file    = RAW_DIR / "demographics.csv"
    comp_file    = RAW_DIR / "competitors.csv"

    missing = [f for f in [permits_file, demo_file, comp_file] if not f.exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing raw data files: {missing}\n"
            "Run fetch_permits.py, fetch_demographics.py, fetch_competitors.py first."
        )

    permits = pd.read_csv(permits_file)
    demo    = pd.read_csv(demo_file)
    comp    = pd.read_csv(comp_file)

    print(f"Loaded: {len(permits)} permit rows, {len(demo)} demo rows, {len(comp)} competitor rows")
    return permits, demo, comp


def build_markets_table(permits: pd.DataFrame) -> pd.DataFrame:
    """Build the master markets dimension table."""
    cities = permits[["city", "state", "cbsa_code"]].drop_duplicates()
    cities["lat"] = cities["city"].map(lambda c: MARKET_CENTROIDS.get(c, (None, None))[0])
    cities["lon"] = cities["city"].map(lambda c: MARKET_CENTROIDS.get(c, (None, None))[1])
    cities["market_id"] = range(1, len(cities) + 1)
    return cities


def create_views(conn: sqlite3.Connection):
    """Create analytical SQL views."""

    conn.execute("DROP VIEW IF EXISTS market_summary")
    conn.execute("""
        CREATE VIEW market_summary AS
        SELECT
            m.market_id,
            m.city,
            m.state,
            m.cbsa_code,
            m.lat,
            m.lon,
            -- Permit data
            p.year               AS permit_year,
            p.total_units        AS permit_total_units,
            p.single_family_units,
            p.multifamily_units,
            p.pct_single_family,
            p.permit_rank,
            -- Demographics
            d.population,
            d.median_household_income,
            d.total_housing_units,
            d.homeownership_rate_pct,
            d.median_home_value,
            -- Competitor counts
            COALESCE(c.total_competitors, 0)    AS total_competitors,
            COALESCE(c.hardware_stores, 0)      AS hardware_stores,
            COALESCE(c.building_material_stores, 0) AS building_material_stores,
            -- Derived KPIs
            ROUND(CAST(p.total_units AS REAL) / d.population * 1000, 2)
                AS permits_per_1k_pop,
            ROUND(CAST(COALESCE(c.total_competitors, 0) AS REAL) / d.population * 100000, 2)
                AS competitors_per_100k,
            -- Opportunity score: high permits + low competitor density = high opportunity
            ROUND(
                (CAST(p.permit_rank AS REAL) * -1 + 16)   -- inverted rank (1st = 15 points)
                + (15 - CAST(p.permit_rank AS REAL))
                + CASE WHEN d.median_household_income > 75000 THEN 3 ELSE 0 END
                + CASE WHEN d.population > 2000000 THEN 2 ELSE 0 END
            , 1) AS opportunity_score
        FROM markets m
        LEFT JOIN permits p     ON m.city = p.city
        LEFT JOIN demographics d ON m.city = d.city
        LEFT JOIN (
            SELECT
                city,
                COUNT(*) AS total_competitors,
                SUM(CASE WHEN category = 'hardware_store'      THEN 1 ELSE 0 END) AS hardware_stores,
                SUM(CASE WHEN category = 'building_materials'  THEN 1 ELSE 0 END) AS building_material_stores
            FROM competitors
            GROUP BY city
        ) c ON m.city = c.city
    """)

    conn.execute("DROP VIEW IF EXISTS top_markets")
    conn.execute("""
        CREATE VIEW top_markets AS
        SELECT
            city,
            state,
            permit_total_units,
            population,
            median_household_income,
            total_competitors,
            permits_per_1k_pop,
            competitors_per_100k,
            opportunity_score,
            RANK() OVER (ORDER BY opportunity_score DESC) AS opportunity_rank
        FROM market_summary
        ORDER BY opportunity_score DESC
    """)

    conn.execute("DROP VIEW IF EXISTS competitor_density")
    conn.execute("""
        CREATE VIEW competitor_density AS
        SELECT
            c.city,
            c.state,
            c.category,
            COUNT(*) AS location_count,
            d.population,
            ROUND(COUNT(*) * 1.0 / d.population * 100000, 2) AS locations_per_100k
        FROM competitors c
        LEFT JOIN demographics d ON c.city = d.city
        GROUP BY c.city, c.state, c.category
        ORDER BY c.city, locations_per_100k DESC
    """)

    print("  Created views: market_summary, top_markets, competitor_density")


def main():
    print("=" * 60)
    print("Building Market Intelligence Database")
    print("=" * 60)

    permits, demo, comp = load_raw_data()
    markets = build_markets_table(permits)

    print(f"\nConnecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)

    # Write tables
    print("Writing tables...")
    markets.to_sql("markets",     conn, if_exists="replace", index=False)
    permits.to_sql("permits",     conn, if_exists="replace", index=False)
    demo.to_sql("demographics",   conn, if_exists="replace", index=False)
    comp.to_sql("competitors",    conn, if_exists="replace", index=False)

    print("  markets:     ", len(markets), "rows")
    print("  permits:     ", len(permits), "rows")
    print("  demographics:", len(demo), "rows")
    print("  competitors: ", len(comp), "rows")

    # Create views
    print("\nCreating SQL views...")
    create_views(conn)

    conn.commit()

    # Preview top markets
    print("\nTop Markets by Opportunity Score:")
    print("-" * 70)
    try:
        top = pd.read_sql("SELECT * FROM top_markets LIMIT 10", conn)
        print(top[["city", "state", "permit_total_units", "opportunity_score", "opportunity_rank"]].to_string(index=False))
    except Exception as e:
        print(f"  (Preview unavailable: {e})")

    conn.close()
    print(f"\nDatabase saved to {DB_PATH}")


if __name__ == "__main__":
    main()
