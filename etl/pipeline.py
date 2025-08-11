"""
pipeline.py
-----------
End-to-end ETL orchestration for the Healthcare Data Pipeline demo.

What it does:
1) EXTRACT  : Reads CSVs for patients, diagnoses, and admissions.
2) TRANSFORM: Cleans data and computes business KPIs:
   - 30-day readmission rate
   - Average Length of Stay (LOS)
   - Admissions by primary diagnosis (Top 10)
   - Daily inpatient census (proxy)
3) LOAD     : Creates tables (if needed) and loads both raw and KPI tables into the warehouse
              (SQLite by default, PostgreSQL if DB_* env vars are set).

This file is intentionally commented for interview readability.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------
# Database Configuration
# ---------------------------
# Default to SQLite for simplicity. If you export DB_* env vars and set DB_DIALECT=postgresql,
# it will connect to a local Postgres (e.g., via docker compose).
DB_DIALECT = os.getenv("DB_DIALECT", "sqlite")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

if DB_DIALECT == "postgresql":
    # SQLAlchemy connection string for Postgres
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    # Local SQLite file as the warehouse
    os.makedirs("warehouse", exist_ok=True)
    conn_str = "sqlite:///warehouse/healthcare.db"

# Create a SQLAlchemy engine (future=True uses newer SQLAlchemy 2.x semantics)
engine = create_engine(conn_str, future=True)


def run_sql_file(path: str) -> None:
    """Run a .sql file against the target database inside a transaction."""
    with open(path, "r") as f, engine.begin() as conn:
        conn.execute(text(f.read()))


def extract(data_dir: str = "data"):
    """Read raw CSV files from the data directory.
    
    Returns:
        (patients_df, diagnoses_df, admissions_df)
    """
    patients = pd.read_csv(f"{data_dir}/patients.csv", parse_dates=["birth_date"])  # birth_date -> datetime
    diagnoses = pd.read_csv(f"{data_dir}/diagnoses.csv")
    admissions = pd.read_csv(f"{data_dir}/admissions.csv", parse_dates=["admit_time", "discharge_time"])  # timestamps
    return patients, diagnoses, admissions


def transform(patients: pd.DataFrame, diagnoses: pd.DataFrame, admissions: pd.DataFrame):
    """Clean and derive KPI datasets.

    Steps:
    - Drop impossible rows (missing or reversed timestamps).
    - Compute Average LOS.
    - Compute 30-day readmission rate by looking at next admit per patient.
    - Aggregate admissions by primary diagnosis.
    - Build a daily inpatient census (proxy) for trend charting.
    """
    # --- Basic cleaning ---
    admissions = admissions.dropna(subset=["admit_time", "discharge_time"])  # remove nulls
    admissions = admissions[admissions["discharge_time"] >= admissions["admit_time"]].copy()  # remove negative LOS

    # The reporting "as-of" date is the most recent discharge in the dataset
    as_of_date = admissions["discharge_time"].max().date()

    # Average Length of Stay (in days)
    los_days = (admissions["discharge_time"] - admissions["admit_time"]).dt.days
    avg_los = float(los_days.mean()) if len(los_days) else 0.0

    # 30-day readmission rate:
    # For each admission, find the NEXT admission for the same patient and check if it
    # occurs within 30 days of the current discharge.
    admissions_sorted = admissions.sort_values(["patient_id", "admit_time"]).copy()
    admissions_sorted["next_admit"] = admissions_sorted.groupby("patient_id")["admit_time"].shift(-1)
    diff_days = (admissions_sorted["next_admit"] - admissions_sorted["discharge_time"]).dt.days
    admissions_sorted["is_readmit_30d"] = diff_days.between(0, 30, inclusive="both")  # include same-day up to 30 days

    # Denominator: discharges that have a next admission
    denom = admissions_sorted["next_admit"].notna().sum()
    # Numerator: of those, how many are within 30 days
    numer = admissions_sorted["is_readmit_30d"].fillna(False).sum()
    readmit_rate = float(numer) / float(denom) if denom > 0 else 0.0

    # Admissions by primary diagnosis (Top 10)
    by_dx = admissions.groupby("primary_diagnosis").size().reset_index(name="admissions_count")
    by_dx = by_dx.sort_values("admissions_count", ascending=False).head(10)

    # Daily census proxy: count how many patients are in-house for each date in the range
    all_days = pd.date_range(admissions["admit_time"].min().date(),
                             admissions["discharge_time"].max().date(), freq="D")
    census = []
    for d in all_days:
        # A patient is "in-house" on date d if admit <= d <= discharge
        in_house = ((admissions["admit_time"].dt.date <= d.date()) &
                    (admissions["discharge_time"].dt.date >= d.date())).sum()
        census.append({"census_date": d.date(), "inpatient_count": int(in_house)})
    census_df = pd.DataFrame(census)

    # Add human-readable diagnosis description
    by_dx = by_dx.merge(diagnoses, left_on="primary_diagnosis", right_on="diagnosis_code", how="left")

    kpi = {
        "as_of_date": as_of_date,
        "avg_los": avg_los,
        "readmit_rate": readmit_rate,
        "by_dx": by_dx,
        "census": census_df
    }
    return patients, diagnoses, admissions, kpi


def load(patients: pd.DataFrame, diagnoses: pd.DataFrame, admissions: pd.DataFrame, kpi: dict) -> None:
    """Create tables and persist raw + KPI datasets into the warehouse."""
    # Create/ensure table structures
    run_sql_file("etl/schema.sql")

    # Replace tables for a clean demo experience
    patients.to_sql("patients", engine, if_exists="replace", index=False)
    diagnoses.to_sql("diagnoses", engine, if_exists="replace", index=False)
    admissions.to_sql("admissions", engine, if_exists="replace", index=False)

    # Upsert KPI tables (simple delete+insert for demo)
    with engine.begin() as conn:
        # Readmission
        conn.execute(text("DELETE FROM kpi_readmission_30d"))
        conn.execute(text("INSERT INTO kpi_readmission_30d(as_of_date, readmission_rate) VALUES (:d, :r)"),
                     {"d": kpi["as_of_date"], "r": kpi["readmit_rate"]})

        # Average LOS
        conn.execute(text("DELETE FROM kpi_avg_los"))
        conn.execute(text("INSERT INTO kpi_avg_los(as_of_date, avg_los_days) VALUES (:d, :r)"),
                     {"d": kpi["as_of_date"], "r": kpi["avg_los"]})

        # Admissions by diagnosis
        conn.execute(text("DELETE FROM kpi_admissions_by_dx"))
        for _, row in kpi["by_dx"].iterrows():
            sql = 'INSERT INTO kpi_admissions_by_dx(as_of_date, diagnosis_code, admissions_count) VALUES (:d, :code, :cnt)'
            conn.execute(text(sql), {"d": kpi["as_of_date"], "code": row["primary_diagnosis"], "cnt": int(row["admissions_count"])})

        # Daily census
        conn.execute(text("DELETE FROM kpi_daily_census"))
        for _, row in kpi["census"].iterrows():
            sql = 'INSERT INTO kpi_daily_census(census_date, inpatient_count) VALUES (:d, :c)'
            conn.execute(text(sql), {"d": row["census_date"], "c": int(row["inpatient_count"])})


def main():
    """Run the full pipeline: extract -> transform -> load."""
    patients, diagnoses, admissions = extract()
    patients, diagnoses, admissions, kpi = transform(patients, diagnoses, admissions)
    load(patients, diagnoses, admissions, kpi)
    print("ETL complete. Warehouse ready.")


if __name__ == "__main__":
    main()
