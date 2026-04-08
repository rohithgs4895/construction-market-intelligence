# Construction Market Intelligence Dashboard

An end-to-end data pipeline and ArcGIS Online dashboard for analyzing building permit activity, competitor density, and market opportunity across the top 15 high-growth US metro markets.

## 🌐 Live Dashboard
[![ArcGIS Dashboard](https://img.shields.io/badge/ArcGIS-Live%20Dashboard-0079C1?style=for-the-badge&logo=arcgis&logoColor=white)](https://www.arcgis.com/apps/dashboards/1693c793a71948029b1ae474e71b667c)

**Live Demo:** https://www.arcgis.com/apps/dashboards/1693c793a71948029b1ae474e71b667c

---

## Overview

This project automates the collection, processing, and visualization of real public datasets to answer one core question: **where are the best construction market opportunities in the US?**

The pipeline pulls live data from the US Census Bureau, OpenStreetMap, and BLS, loads it into a SQLite database with analytical SQL views, exports shapefiles, and publishes them as hosted feature layers on ArcGIS Online — powering an interactive market intelligence dashboard.

---

## Live Dashboard

[![Construction Market Intelligence Dashboard](https://img.shields.io/badge/ArcGIS-Dashboard-0079C1?style=for-the-badge&logo=arcgis&logoColor=white)](https://www.arcgis.com/apps/dashboards/1693c793a71948029b1ae474e71b667c)

**[https://www.arcgis.com/apps/dashboards/1693c793a71948029b1ae474e71b667c](https://www.arcgis.com/apps/dashboards/1693c793a71948029b1ae474e71b667c)**

---

## Key Metrics

| Metric | Value |
|---|---|
| Markets analyzed | 15 US metros |
| Building permits (2023) | 458,000+ units authorized |
| Competitor locations | 597 (from OpenStreetMap) |
| ArcGIS feature layers | 4 hosted layers |
| Demographics coverage | 100% of target markets |

---

## Markets Covered

| City | State | 2023 Permits | Opportunity Tier |
|---|---|---|---|
| Dallas | TX | 58,200 | Tier 1 |
| Houston | TX | 55,600 | Tier 1 |
| Phoenix | AZ | 42,800 | Tier 1 |
| Atlanta | GA | 41,300 | Tier 1 |
| Austin | TX | 38,900 | Tier 2 |
| Orlando | FL | 29,800 | Tier 2 |
| Charlotte | NC | 28,700 | Tier 2 |
| Tampa | FL | 24,100 | Tier 3 |
| Nashville | TN | 22,400 | Tier 3 |
| Las Vegas | NV | 21,300 | Tier 3 |
| Raleigh | NC | 21,900 | Tier 3 |
| San Antonio | TX | 19,800 | Tier 3 |
| Denver | CO | 18,600 | Tier 3 |
| Jacksonville | FL | 18,200 | Tier 3 |
| Fort Worth | TX | 16,400 | Tier 4 |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data pipeline | Python 3.x (pandas, geopandas, requests, overpy) |
| Geospatial | GeoPandas, Shapely, Fiona, GDAL |
| Database | SQLite + SQLAlchemy |
| ArcGIS integration | ArcGIS API for Python (`arcgis`) |
| Visualization | ArcGIS Online Dashboards |
| Data — permits | US Census Bureau Building Permits Survey (BPS) |
| Data — demographics | US Census American Community Survey (ACS 5-year) |
| Data — competitors | OpenStreetMap via Overpass API (`overpy`) |

---

## Data Sources

| Source | Dataset | Coverage |
|---|---|---|
| [US Census BPS](https://www.census.gov/construction/bps/) | Annual building permits by metro | 2023, 15 CBSAs |
| [Census ACS 5-year](https://www.census.gov/programs-surveys/acs) | Population, income, housing units | 2022, 15 CBSAs |
| [OpenStreetMap / Overpass](https://overpass-api.de/) | Hardware stores, building suppliers, contractors | Current, 15 metro bounding boxes |

Census data is fetched live via the Census Bureau API. No API key required for low-volume use; set `CENSUS_API_KEY` in `.env` for higher rate limits.

---

## Project Structure

```
construction-market-intelligence/
│
├── data/
│   ├── raw/                        # Downloaded source files
│   │   ├── building_permits.csv    # Census BPS 2023 permit counts
│   │   ├── demographics.csv        # Census ACS population/income/housing
│   │   └── competitors.csv         # OSM competitor locations (597 records)
│   └── processed/
│       └── market_intelligence.db  # SQLite database with views
│
├── src/
│   ├── fetch_permits.py            # Step 1: Census BPS permit data
│   ├── fetch_demographics.py       # Step 2: Census ACS demographics
│   ├── fetch_competitors.py        # Step 3: OpenStreetMap via Overpass API
│   ├── build_database.py           # Step 4: SQLite DB + analytical views
│   ├── create_feature_layers.py    # Step 5: Export shapefiles for ArcGIS
│   └── publish_to_arcgis.py        # Step 6: Publish to ArcGIS Online
│
├── outputs/                        # Generated shapefiles (ArcGIS-ready)
│   ├── market_boundaries.*         # 15 metro centroids + all KPIs
│   ├── permit_hotspots.*           # Permit tiers (Tier 1–4)
│   ├── competitor_locations.*      # 597 individual business points
│   └── market_summary.*            # Opportunity scores (primary map layer)
│
├── arcgis/
│   └── published_layers.txt        # Item IDs of published feature layers
│
├── dashboard/                      # Dashboard config / screenshots
├── docs/                           # Additional documentation
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Database Schema

**Tables:** `markets`, `permits`, `demographics`, `competitors`

**SQL Views:**

| View | Description |
|---|---|
| `market_summary` | One row per market — all KPIs joined (permits + demographics + competitor count) |
| `top_markets` | Markets ranked by composite opportunity score |
| `competitor_density` | Competitor count per 100k population by city and category |

**Opportunity score** is a composite of permit volume rank, population size, and median household income. Higher = better market entry opportunity.

---

## ArcGIS Feature Layers

| Layer Name | Content | Geometry |
|---|---|---|
| `CMI_Market_Boundaries` | 15 metros with full KPI attributes | Point |
| `CMI_Permit_Hotspots` | Permit volume with Tier 1–4 classification | Point |
| `CMI_Competitor_Locations` | 597 individual business locations | Point |
| `CMI_Market_Summary` | Opportunity scores — primary dashboard layer | Point |

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/rohithgs4895/construction-market-intelligence.git
cd construction-market-intelligence
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. (Optional) Configure Census API key

Create a `.env` file in the project root:

```
CENSUS_API_KEY=your_key_here
ARCGIS_USERNAME=your_arcgis_username
ARCGIS_PASSWORD=your_arcgis_password
```

Get a free Census API key at [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html)

---

## Running the Pipeline

Run each script in order:

```bash
# Step 1–3: Fetch raw data from public APIs
python src/fetch_permits.py          # Census BPS building permits
python src/fetch_demographics.py     # Census ACS demographics
python src/fetch_competitors.py      # OpenStreetMap competitor locations

# Step 4: Build SQLite database and SQL views
python src/build_database.py

# Step 5: Export shapefiles for ArcGIS
python src/create_feature_layers.py

# Step 6: Publish to ArcGIS Online (prompts for credentials if not in .env)
python src/publish_to_arcgis.py
```

> **Note on Overpass API rate limits:** `fetch_competitors.py` uses a 15-second delay between markets. If you encounter `Too many requests` errors, the script will automatically retry up to 3 times with 30-second backoff.

---

## Outputs

After running the full pipeline:

- `data/raw/` — three CSVs with source data
- `data/processed/market_intelligence.db` — SQLite database (4 tables, 3 views)
- `outputs/` — four shapefiles ready for ArcGIS Online upload
- `arcgis/published_layers.txt` — ArcGIS item IDs and REST endpoint URLs

---

## License

Data sourced from US Census Bureau and OpenStreetMap is public domain / [ODbL](https://opendatacommons.org/licenses/odbl/). Code in this repository is available under the MIT License.
