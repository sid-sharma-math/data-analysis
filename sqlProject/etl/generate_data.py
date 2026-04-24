"""
generate_data.py

Creates synthetic OVB-style data and writes one CSV per table into ../data/.
Deterministic: same seed -> same CSVs every run.

Run (from the sqlProject folder, with the venv activated):
    python etl/generate_data.py
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker


# ---------------------------------------------------------------------------
# Config — tweak these to scale the dataset up or down.
# ---------------------------------------------------------------------------
N_ADVISERS  = 50
N_CLIENTS   = 1_500
N_CONTRACTS = 10_000
DATE_START  = date(2020, 1, 1)
DATE_END    = date(2026, 12, 31)

SEED = 42   # deterministic runs — same output every time

# Where to write the CSVs. __file__ is this script; .parent is etl/;
# .parent.parent is sqlProject/. Lets the script run from any cwd.
DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# ---------------------------------------------------------------------------
# Domain — realistic German business context
# ---------------------------------------------------------------------------
GERMAN_CITIES = [
    ("Köln", 0.15), ("Hamburg", 0.12), ("Berlin", 0.14), ("München", 0.12),
    ("Frankfurt", 0.10), ("Düsseldorf", 0.08), ("Stuttgart", 0.07),
    ("Bochum", 0.04), ("Essen", 0.04), ("Dortmund", 0.04), ("Leipzig", 0.04),
    ("Hannover", 0.03), ("Nürnberg", 0.03),
]

EXPERIENCE_LEVELS = [("Junior", 0.40), ("Mid", 0.40), ("Senior", 0.20)]

ACQUISITION_CHANNELS = [
    ("Referral", 0.45), ("Web", 0.25), ("Partner", 0.20), ("Event", 0.10),
]

# (product_name, category, provider, risk_level)
PRODUCTS = [
    ("Riester-Rente Basis",            "Pension",    "Allianz",    "Low"),
    ("Rürup-Rente Plus",               "Pension",    "Allianz",    "Low"),
    ("Betriebsrente Classic",          "Pension",    "Generali",   "Low"),
    ("Private Rentenversicherung",     "Pension",    "Zurich",     "Low"),
    ("Risikolebensversicherung",       "Insurance",  "HUK-Coburg", "Low"),
    ("Berufsunfähigkeitsversicherung", "Insurance",  "Allianz",    "Low"),
    ("Kfz-Versicherung Premium",       "Insurance",  "HUK-Coburg", "Low"),
    ("Hausratversicherung",            "Insurance",  "Generali",   "Low"),
    ("Haftpflichtversicherung",        "Insurance",  "Zurich",     "Low"),
    ("Private Krankenzusatz",          "Insurance",  "DKV",        "Low"),
    ("DWS TopDividende Fonds",         "Investment", "DWS",        "Medium"),
    ("MSCI World ETF Sparplan",        "Investment", "iShares",    "Medium"),
    ("Deka Global Champions",          "Investment", "Deka",       "Medium"),
    ("Emerging Markets Equity Fund",   "Investment", "DWS",        "High"),
    ("Nachhaltiger Aktien ETF",        "Investment", "iShares",    "Medium"),
    ("Tagesgeld Plus",                 "Investment", "Volksbank",  "Low"),
    ("Baufinanzierung 10J",            "Mortgage",   "Sparkasse",  "Medium"),
    ("Baufinanzierung 15J",            "Mortgage",   "ING",        "Medium"),
    ("Anschlussfinanzierung",          "Mortgage",   "Volksbank",  "Medium"),
    ("Modernisierungskredit",          "Mortgage",   "Sparkasse",  "Medium"),
]

# Contract-value range (EUR) by product category
VALUE_RANGES = {
    "Insurance":  (5_000,   150_000),
    "Pension":    (20_000,  400_000),
    "Investment": (2_000,   250_000),
    "Mortgage":   (100_000, 800_000),
}

# Typical adviser commission as a fraction of contract value
COMMISSION_PCT = {"Insurance": 0.04, "Pension": 0.05, "Investment": 0.02, "Mortgage": 0.01}

# Whether the product category has a monthly premium column
HAS_MONTHLY_PREMIUM = {"Insurance": True, "Pension": True, "Investment": False, "Mortgage": True}

STATUS_WEIGHTS = [("Active", 0.75), ("Completed", 0.15), ("Cancelled", 0.10)]

MONTH_NAMES_DE = ["Januar", "Februar", "März", "April", "Mai", "Juni",
                  "Juli", "August", "September", "Oktober", "November", "Dezember"]
DAY_NAMES_DE   = ["Montag", "Dienstag", "Mittwoch", "Donnerstag",
                  "Freitag", "Samstag", "Sonntag"]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def weighted_pick(items):
    """items: list of (value, weight) pairs. Returns one value."""
    values, weights = zip(*items)
    return random.choices(values, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Generators — one per table
# ---------------------------------------------------------------------------
def generate_advisers(fake: Faker) -> pd.DataFrame:
    rows = []
    for i in range(1, N_ADVISERS + 1):
        experience = weighted_pick(EXPERIENCE_LEVELS)
        # Seniors were hired longer ago; juniors more recently.
        if experience == "Senior":
            hire = fake.date_between(date(2010, 1, 1), date(2018, 12, 31))
        elif experience == "Mid":
            hire = fake.date_between(date(2018, 1, 1), date(2022, 12, 31))
        else:
            hire = fake.date_between(date(2022, 1, 1), date(2025, 12, 31))

        rows.append({
            "adviser_sk":       i,
            "adviser_id":       f"ADV-{i:05d}",
            "first_name":       fake.first_name(),
            "last_name":        fake.last_name(),
            "region":           weighted_pick(GERMAN_CITIES),
            "experience_level": experience,
            "hire_date":        hire,
            "is_active":        random.choices([1, 0], weights=[0.9, 0.1])[0],
        })
    return pd.DataFrame(rows)


def generate_clients(fake: Faker) -> pd.DataFrame:
    rows = []
    for i in range(1, N_CLIENTS + 1):
        gender = random.choices(["Male", "Female", None], weights=[0.48, 0.48, 0.04])[0]
        if gender == "Female":
            first = fake.first_name_female()
        elif gender == "Male":
            first = fake.first_name_male()
        else:
            first = fake.first_name()

        rows.append({
            "client_sk":           i,
            "client_id":           f"CL-{i:06d}",
            "first_name":          first,
            "last_name":           fake.last_name(),
            "date_of_birth":       fake.date_of_birth(minimum_age=18, maximum_age=85),
            "gender":              gender,
            "city":                weighted_pick(GERMAN_CITIES),
            "country":             random.choices(
                                        ["Germany", "Austria", "Switzerland"],
                                        weights=[0.92, 0.05, 0.03])[0],
            "acquisition_channel": weighted_pick(ACQUISITION_CHANNELS),
        })
    return pd.DataFrame(rows)


def generate_products() -> pd.DataFrame:
    rows = []
    for i, (name, cat, provider, risk) in enumerate(PRODUCTS, start=1):
        rows.append({
            "product_sk":       i,
            "product_id":       f"PRD-{i:04d}",
            "product_name":     name,
            "product_category": cat,
            "provider":         provider,
            "risk_level":       risk,
        })
    return pd.DataFrame(rows)


def generate_dates() -> pd.DataFrame:
    rows = []
    d = DATE_START
    while d <= DATE_END:
        dow = d.isoweekday()  # Mon=1 .. Sun=7

        # Last day of month = first of next month - 1 day.
        # Using day=28 avoids month-end ambiguity on the way there.
        next_month    = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
        last_of_month = next_month - timedelta(days=1)

        rows.append({
            "date_sk":      int(d.strftime("%Y%m%d")),   # 20260423 etc.
            "full_date":    d,
            "day_num":      d.day,
            "month_num":    d.month,
            "month_name":   MONTH_NAMES_DE[d.month - 1],
            "quarter_num":  (d.month - 1) // 3 + 1,
            "year_num":     d.year,
            "day_of_week":  dow,
            "day_name":     DAY_NAMES_DE[dow - 1],
            "is_weekend":   1 if dow >= 6 else 0,
            "is_month_end": 1 if d == last_of_month else 0,
        })
        d += timedelta(days=1)
    return pd.DataFrame(rows)


def generate_contracts(advisers: pd.DataFrame, clients: pd.DataFrame,
                       products: pd.DataFrame, dates: pd.DataFrame) -> pd.DataFrame:
    # Only active advisers sign new contracts.
    active_advisers = advisers.loc[advisers["is_active"] == 1, "adviser_sk"].tolist()
    client_sks      = clients["client_sk"].tolist()
    product_sks     = products["product_sk"].tolist()
    product_info    = products.set_index("product_sk").to_dict("index")

    # Weight signing dates: weekdays much more likely than weekends.
    date_sks     = dates["date_sk"].tolist()
    date_weights = [0.1 if w else 1.0 for w in dates["is_weekend"].tolist()]

    rows = []
    for i in range(1, N_CONTRACTS + 1):
        product_sk = random.choice(product_sks)
        category   = product_info[product_sk]["product_category"]

        vmin, vmax     = VALUE_RANGES[category]
        contract_value = round(random.uniform(vmin, vmax), 2)
        # Commission = category %, wobbled ±20% so the numbers aren't too uniform.
        commission     = round(contract_value * COMMISSION_PCT[category] * random.uniform(0.8, 1.2), 2)

        if HAS_MONTHLY_PREMIUM[category]:
            duration        = random.choice([60, 120, 180, 240, 300, 360])   # 5–30 yrs
            premium_monthly = round(contract_value / duration, 2)
        else:
            duration        = random.choice([12, 24, 36, 60, 120])           # 1–10 yrs
            premium_monthly = None

        rows.append({
            "contract_sk":     i,
            "contract_id":     f"C-{i:07d}",
            "adviser_sk":      random.choice(active_advisers),
            "client_sk":       random.choice(client_sks),
            "product_sk":      product_sk,
            "signing_date_sk": random.choices(date_sks, weights=date_weights, k=1)[0],
            "contract_value":  contract_value,
            "commission":      commission,
            "premium_monthly": premium_monthly,
            "duration_months": duration,
            "status":          weighted_pick(STATUS_WEIGHTS),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    random.seed(SEED)
    Faker.seed(SEED)
    fake = Faker("de_DE")

    DATA_DIR.mkdir(exist_ok=True)

    print("Generating advisers...")
    advisers = generate_advisers(fake)
    advisers.to_csv(DATA_DIR / "dim_adviser.csv", index=False)

    print("Generating clients...")
    clients = generate_clients(fake)
    clients.to_csv(DATA_DIR / "dim_client.csv", index=False)

    print("Generating products...")
    products = generate_products()
    products.to_csv(DATA_DIR / "dim_product.csv", index=False)

    print("Generating date dimension...")
    dates = generate_dates()
    dates.to_csv(DATA_DIR / "dim_date.csv", index=False)

    print("Generating contracts...")
    contracts = generate_contracts(advisers, clients, products, dates)
    contracts.to_csv(DATA_DIR / "fact_contracts.csv", index=False)

    print("\nSummary:")
    print(f"  advisers  : {len(advisers):>7,}")
    print(f"  clients   : {len(clients):>7,}")
    print(f"  products  : {len(products):>7,}")
    print(f"  dates     : {len(dates):>7,}")
    print(f"  contracts : {len(contracts):>7,}")
    print(f"\nCSVs written to: {DATA_DIR}")


if __name__ == "__main__":
    main()
