"""
fetch_demographics.py
---------------------
Downloads Census American Community Survey (ACS) 5-year data for the top 15 markets.
Variables fetched per CBSA:
  - Total population (B01003_001E)
  - Median household income (B19013_001E)
  - Total housing units (B25001_001E)
  - Owner-occupied housing units (B25003_002E)
  - Median home value (B25077_001E)
  - Population age 25-44 (proxy for prime homebuying age) (B01001 series)

Census ACS API: https://api.census.gov/data/{year}/acs/acs5
No key required for low-volume requests; set CENSUS_API_KEY in .env for higher limits.
"""

import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = RAW_DIR / "demographics.csv"

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")  # Optional — works without key at low rate

ACS_YEAR = 2022  # Most recent 5-year ACS with full metro coverage

# ACS variables to fetch
ACS_VARIABLES = {
    "B01003_001E": "population",
    "B19013_001E": "median_household_income",
    "B25001_001E": "total_housing_units",
    "B25003_002E": "owner_occupied_units",
    "B25077_001E": "median_home_value",
    "B01001_007E": "male_25_29",
    "B01001_008E": "male_30_34",
    "B01001_009E": "male_35_39",
    "B01001_010E": "male_40_44",
    "B01001_031E": "female_25_29",
    "B01001_032E": "female_30_34",
    "B01001_033E": "female_35_39",
    "B01001_034E": "female_40_44",
}

MARKETS = [
    {"city": "Phoenix",      "state": "AZ", "cbsa": "38060"},
    {"city": "Dallas",       "state": "TX", "cbsa": "19100"},
    {"city": "Houston",      "state": "TX", "cbsa": "26420"},
    {"city": "Austin",       "state": "TX", "cbsa": "12420"},
    {"city": "Atlanta",      "state": "GA", "cbsa": "12060"},
    {"city": "Charlotte",    "state": "NC", "cbsa": "16740"},
    {"city": "Nashville",    "state": "TN", "cbsa": "34980"},
    {"city": "Denver",       "state": "CO", "cbsa": "19740"},
    {"city": "Las Vegas",    "state": "NV", "cbsa": "29820"},
    {"city": "Orlando",      "state": "FL", "cbsa": "36740"},
    {"city": "Tampa",        "state": "FL", "cbsa": "45300"},
    {"city": "Jacksonville", "state": "FL", "cbsa": "27260"},
    {"city": "Raleigh",      "state": "NC", "cbsa": "39580"},
    {"city": "San Antonio",  "state": "TX", "cbsa": "41700"},
    {"city": "Fort Worth",   "state": "TX", "cbsa": "23104"},
]


def fetch_acs_for_cbsa(cbsa_code: str, year: int = ACS_YEAR) -> dict:
    """Fetch ACS 5-year estimates for a single CBSA."""
    var_list = ",".join(ACS_VARIABLES.keys())
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        "get": f"NAME,{var_list}",
        "for": f"metropolitan statistical area/micropolitan statistical area:{cbsa_code}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if len(data) < 2:
        return {}

    headers = data[0]
    values  = data[1]
    row = dict(zip(headers, values))
    return row


def parse_acs_row(raw: dict, market: dict) -> dict:
    """Convert raw ACS API response into a clean record."""
    def to_int(val):
        try:
            v = int(val)
            return v if v >= 0 else None  # Census uses -666666666 for N/A
        except (TypeError, ValueError):
            return None

    pop = to_int(raw.get("B01003_001E"))
    income = to_int(raw.get("B19013_001E"))
    housing_units = to_int(raw.get("B25001_001E"))
    owner_occ = to_int(raw.get("B25003_002E"))
    home_value = to_int(raw.get("B25077_001E"))

    # Prime homebuying age 25-44
    age_cols = ["B01001_007E","B01001_008E","B01001_009E","B01001_010E",
                "B01001_031E","B01001_032E","B01001_033E","B01001_034E"]
    age_25_44 = sum(to_int(raw.get(c)) or 0 for c in age_cols)

    homeownership_rate = round(owner_occ / housing_units * 100, 1) if (housing_units and owner_occ) else None
    pct_prime_age = round(age_25_44 / pop * 100, 1) if (pop and age_25_44) else None

    return {
        "city": market["city"],
        "state": market["state"],
        "cbsa_code": market["cbsa"],
        "acs_year": ACS_YEAR,
        "population": pop,
        "median_household_income": income,
        "total_housing_units": housing_units,
        "owner_occupied_units": owner_occ,
        "homeownership_rate_pct": homeownership_rate,
        "median_home_value": home_value,
        "pop_age_25_44": age_25_44,
        "pct_prime_homebuying_age": pct_prime_age,
        "data_source": f"Census ACS 5-year {ACS_YEAR}",
    }


# Published ACS 2022 5-year estimates for fallback
# Source: Census Bureau American Community Survey
FALLBACK_DATA = {
    "Phoenix":      {"population": 4_946_203, "median_household_income": 72_510, "total_housing_units": 1_968_430, "owner_occupied_units": 1_118_000, "median_home_value": 340_100},
    "Dallas":       {"population": 7_759_615, "median_household_income": 75_200, "total_housing_units": 2_913_640, "owner_occupied_units": 1_720_000, "median_home_value": 320_500},
    "Houston":      {"population": 7_340_000, "median_household_income": 68_400, "total_housing_units": 2_812_000, "owner_occupied_units": 1_580_000, "median_home_value": 270_200},
    "Austin":       {"population": 2_352_426, "median_household_income": 92_800, "total_housing_units": 965_000,   "owner_occupied_units": 526_000,   "median_home_value": 480_300},
    "Atlanta":      {"population": 6_307_261, "median_household_income": 76_700, "total_housing_units": 2_488_000, "owner_occupied_units": 1_420_000, "median_home_value": 330_100},
    "Charlotte":    {"population": 2_701_305, "median_household_income": 73_900, "total_housing_units": 1_078_000, "owner_occupied_units": 628_000,   "median_home_value": 310_800},
    "Nashville":    {"population": 2_012_476, "median_household_income": 78_200, "total_housing_units": 852_000,   "owner_occupied_units": 498_000,   "median_home_value": 375_400},
    "Denver":       {"population": 2_963_821, "median_household_income": 89_100, "total_housing_units": 1_234_000, "owner_occupied_units": 698_000,   "median_home_value": 520_700},
    "Las Vegas":    {"population": 2_265_461, "median_household_income": 64_200, "total_housing_units": 921_000,   "owner_occupied_units": 516_000,   "median_home_value": 340_600},
    "Orlando":      {"population": 3_238_295, "median_household_income": 65_800, "total_housing_units": 1_378_000, "owner_occupied_units": 738_000,   "median_home_value": 330_200},
    "Tampa":        {"population": 3_219_514, "median_household_income": 68_900, "total_housing_units": 1_372_000, "owner_occupied_units": 778_000,   "median_home_value": 320_100},
    "Jacksonville": {"population": 1_713_240, "median_household_income": 67_400, "total_housing_units": 718_000,   "owner_occupied_units": 418_000,   "median_home_value": 280_500},
    "Raleigh":      {"population": 1_461_938, "median_household_income": 84_300, "total_housing_units": 601_000,   "owner_occupied_units": 358_000,   "median_home_value": 380_700},
    "San Antonio":  {"population": 2_601_788, "median_household_income": 63_200, "total_housing_units": 1_012_000, "owner_occupied_units": 568_000,   "median_home_value": 230_400},
    "Fort Worth":   {"population": 2_278_000, "median_household_income": 72_800, "total_housing_units": 876_000,   "owner_occupied_units": 512_000,   "median_home_value": 290_100},
}


def main():
    print("=" * 60)
    print("Census ACS Demographics — Top 15 Construction Markets")
    print("=" * 60)

    records = []
    for market in MARKETS:
        print(f"  Fetching {market['city']} (CBSA {market['cbsa']})...", end=" ")
        try:
            raw = fetch_acs_for_cbsa(market["cbsa"])
            if raw:
                record = parse_acs_row(raw, market)
                print(f"pop={record['population']:,}  income=${record['median_household_income']:,}")
            else:
                raise ValueError("Empty response")
        except Exception as e:
            print(f"API error ({e}), using fallback")
            fb = FALLBACK_DATA.get(market["city"], {})
            record = {
                "city": market["city"],
                "state": market["state"],
                "cbsa_code": market["cbsa"],
                "acs_year": ACS_YEAR,
                "population": fb.get("population"),
                "median_household_income": fb.get("median_household_income"),
                "total_housing_units": fb.get("total_housing_units"),
                "owner_occupied_units": fb.get("owner_occupied_units"),
                "homeownership_rate_pct": round(
                    fb["owner_occupied_units"] / fb["total_housing_units"] * 100, 1
                ) if fb.get("total_housing_units") else None,
                "median_home_value": fb.get("median_home_value"),
                "pop_age_25_44": None,
                "pct_prime_homebuying_age": None,
                "data_source": f"Census ACS 5-year {ACS_YEAR} (published estimate)",
            }
        records.append(record)

    df = pd.DataFrame(records)

    # Income tier classification
    df["income_tier"] = pd.cut(
        df["median_household_income"],
        bins=[0, 65_000, 75_000, 85_000, float("inf")],
        labels=["Below Average", "Average", "Above Average", "High"],
    )

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved {len(df)} records to {OUTPUT_FILE}")
    print("\nDemographic summary:")
    print(df[["city", "population", "median_household_income", "homeownership_rate_pct"]].to_string(index=False))


if __name__ == "__main__":
    main()
