"""
load_data.py

Reads the CSVs produced by generate_data.py and loads them into the
OVBAnalytics database on the local SQL Server. Idempotent — deletes
existing rows first, so repeated runs produce the same end state.

Usage (venv activated, run from sqlProject/):
    python etl/load_data.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SERVER   = r"localhost\SQLEXPRESS"
DATABASE = "OVBAnalytics"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Load order respects foreign keys: every dimension must exist before the
# fact table that references it.
LOAD_PLAN = [
    # (table_name,     csv_file,             date_cols,         has_identity)
    # dim_date has a natural PK (date_sk = YYYYMMDD), NOT an auto-generated
    # IDENTITY column. IDENTITY_INSERT and DBCC CHECKIDENT don't apply to
    # it, so we flag it False and skip those operations for this table.
    ("dim_date",       "dim_date.csv",       ["full_date"],     False),
    ("dim_adviser",    "dim_adviser.csv",    ["hire_date"],     True),
    ("dim_client",     "dim_client.csv",     ["date_of_birth"], True),
    ("dim_product",    "dim_product.csv",    [],                True),
    ("fact_contracts", "fact_contracts.csv", [],                True),
]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def pick_odbc_driver() -> str:
    """
    Find the newest installed Microsoft SQL Server ODBC driver.

    pyodbc.drivers() returns every ODBC driver registered on the machine.
    SQL Server typically installs one of the 'ODBC Driver N for SQL Server'
    variants. We prefer the newest available.
    """
    installed = pyodbc.drivers()
    preferences = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
    ]
    for driver in preferences:
        if driver in installed:
            return driver
    raise RuntimeError(
        f"No Microsoft SQL Server ODBC driver found on this machine.\n"
        f"Installed drivers: {installed}\n"
        f"Install 'Microsoft ODBC Driver 18 for SQL Server' from "
        f"https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
    )


def build_engine() -> Engine:
    driver = pick_odbc_driver()
    print(f"Using ODBC driver: {driver}")

    # Spaces in the driver name are URL-encoded as '+' in SQLAlchemy URLs.
    driver_urlenc = driver.replace(" ", "+")

    # Anatomy of the connection string:
    #   mssql+pyodbc                 -> dialect (mssql) and driver (pyodbc)
    #   @localhost\SQLEXPRESS        -> server\instance; empty user because Windows Auth
    #   /OVBAnalytics                -> default database to connect to
    #   ?driver=...                  -> which ODBC driver to use
    #   &Trusted_Connection=yes      -> use Windows credentials, no username/password
    #   &TrustServerCertificate=yes  -> accept the server's self-signed TLS cert (dev only)
    conn_str = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE}"
        f"?driver={driver_urlenc}"
        "&Trusted_Connection=yes"
        "&TrustServerCertificate=yes"
    )

    # fast_executemany=True uses a native bulk-insert protocol instead of
    # emitting one parameterised INSERT per row. Orders of magnitude faster
    # for the 10k-row fact table.
    return create_engine(conn_str, fast_executemany=True)


# ---------------------------------------------------------------------------
# Load logic
# ---------------------------------------------------------------------------
def nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    pandas uses NaN/NaT to represent missing values. pyodbc wants Python's
    None to translate to SQL NULL. Converting everything to object-dtype
    and replacing NaN with None achieves that uniformly.
    """
    return df.astype(object).where(pd.notna(df), None)


def load_table(conn, table: str, csv_path: Path, date_cols: list[str], has_identity: bool) -> int:
    df = pd.read_csv(csv_path, parse_dates=date_cols)
    df = nan_to_none(df)

    # SQL Server normally assigns IDENTITY column values (our *_sk columns)
    # automatically. IDENTITY_INSERT ON lets us override that and insert
    # specific values — required because we pre-assigned surrogate keys in
    # generate_data.py so the fact table can reference them.
    # Only ONE table per session can have IDENTITY_INSERT ON at a time, so
    # we toggle it around each load. Tables without an IDENTITY column
    # (like dim_date) reject IDENTITY_INSERT entirely — skip it for those.
    if has_identity:
        conn.execute(text(f"SET IDENTITY_INSERT dbo.{table} ON"))
    df.to_sql(
        table,
        conn,
        schema="dbo",
        if_exists="append",   # keep the existing table definition, just add rows
        index=False,          # don't write the pandas index as a column
        chunksize=500,        # how many rows to send per batch
    )
    if has_identity:
        conn.execute(text(f"SET IDENTITY_INSERT dbo.{table} OFF"))
    return len(df)


def clear_all_tables(conn) -> None:
    """
    Wipe rows so re-runs land in a clean state. Order matters — the fact
    table must go first because its foreign keys point at the dimensions,
    so iterate LOAD_PLAN in reverse.

    DELETE instead of TRUNCATE because TRUNCATE is forbidden on tables with
    incoming foreign keys. DBCC CHECKIDENT resets the IDENTITY counter to 0
    so the next load starts surrogate keys at 1 again — but it's only valid
    for tables that actually have an IDENTITY column.
    """
    for table, _, _, has_identity in reversed(LOAD_PLAN):
        conn.execute(text(f"DELETE FROM dbo.{table}"))
        if has_identity:
            conn.execute(text(f"DBCC CHECKIDENT ('dbo.{table}', RESEED, 0)"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # Sanity check — all CSVs must exist before we touch the database.
    missing = [f for _, f, _, _ in LOAD_PLAN if not (DATA_DIR / f).exists()]
    if missing:
        print(f"Missing CSV files in {DATA_DIR}: {missing}", file=sys.stderr)
        print("Run: python etl/generate_data.py  first.", file=sys.stderr)
        sys.exit(1)

    engine = build_engine()

    # engine.begin() opens one connection and wraps everything in a single
    # transaction. If anything fails, the whole load rolls back — so you're
    # never left with half-loaded data.
    with engine.begin() as conn:
        print("Clearing existing rows...")
        clear_all_tables(conn)

        for table, csv_file, date_cols, has_identity in LOAD_PLAN:
            print(f"Loading {table:20s} from {csv_file} ... ", end="", flush=True)
            n = load_table(conn, table, DATA_DIR / csv_file, date_cols, has_identity)
            print(f"{n:>7,} rows")

    # Independent verification — count rows after the transaction has committed.
    print("\nRow counts in database:")
    with engine.connect() as conn:
        for table, _, _, _ in LOAD_PLAN:
            n = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{table}")).scalar()
            print(f"  {table:20s}: {n:>7,}")
    print("\nLoad complete.")


if __name__ == "__main__":
    main()
