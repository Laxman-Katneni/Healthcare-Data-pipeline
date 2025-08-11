"""
streamlit_app.py
----------------
Interactive dashboard for visualizing KPIs produced by the ETL pipeline.

How to use:
1) Run the ETL first to populate the warehouse (SQLite or PostgreSQL).
2) Launch Streamlit with: `streamlit run dashboard/streamlit_app.py`
3) Explore KPIs and charts.

This file is intentionally commented for interview clarity.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st

# Configure Streamlit page
st.set_page_config(page_title="Healthcare KPIs", layout="wide")

# ---------------------------
# Database Configuration
# ---------------------------
DB_DIALECT = os.getenv("DB_DIALECT", "sqlite")
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_NAME = os.getenv("DB_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

if DB_DIALECT == "postgresql":
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    # SQLite fallback for a no-setup demo
    conn_str = "sqlite:///warehouse/healthcare.db"

engine = create_engine(conn_str, future=True)

# ---------------------------
# Header + Intro
# ---------------------------
st.title("🏥 Healthcare Operations Dashboard")
st.caption("KPIs generated via Python ETL (pandas + SQLAlchemy). Synthetic data only.")

# ---------------------------
# Top-level KPIs
# ---------------------------
# We read the most recent records from the KPI tables (ETL writes them each run).
kpi_readm = pd.read_sql("SELECT * FROM kpi_readmission_30d ORDER BY as_of_date DESC LIMIT 1", engine)
kpi_los = pd.read_sql("SELECT * FROM kpi_avg_los ORDER BY as_of_date DESC LIMIT 1", engine)

col1, col2 = st.columns(2)
if not kpi_readm.empty:
    col1.metric("30-Day Readmission Rate", f"{kpi_readm.iloc[0]['readmission_rate']*100:.1f}%")
if not kpi_los.empty:
    col2.metric("Average LOS (days)", f"{kpi_los.iloc[0]['avg_los_days']:.2f}")

st.divider()

# ---------------------------
# Admissions by Diagnosis (Top 10)
# ---------------------------
st.subheader("Admissions by Primary Diagnosis (Top 10)")
by_dx = pd.read_sql("""
SELECT d.diagnosis_desc, k.admissions_count
FROM kpi_admissions_by_dx k
LEFT JOIN diagnoses d ON d.diagnosis_code = k.diagnosis_code
ORDER BY k.admissions_count DESC
""", engine)
# Streamlit can chart directly from a DataFrame indexed by the label column
if not by_dx.empty:
    st.bar_chart(by_dx.set_index("diagnosis_desc"))

# ---------------------------
# Daily Inpatient Census (Proxy)
# ---------------------------
st.subheader("Daily Inpatient Census (Proxy)")
census = pd.read_sql("SELECT * FROM kpi_daily_census ORDER BY census_date", engine)
if not census.empty:
    census["census_date"] = pd.to_datetime(census["census_date"])  # ensure datetime
    census = census.set_index("census_date")
    st.line_chart(census["inpatient_count"])

st.divider()
st.caption("Switch to PostgreSQL by setting DB_* environment variables and re-running the ETL.")
