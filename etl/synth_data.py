"""
synth_data.py
-------------
Synthetic data generator for the Healthcare Data Pipeline demo.
- Patients: random gender + birth_date
- Diagnoses: fixed ICD-like codes/descriptions
- Admissions: random admit/discharge with gamma-distributed LOS and primary diagnosis

All data is fake; no PHI.
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import random

# A small set of ICD-like diagnosis codes and descriptions
DIAGNOSES = [
    ("I10", "Essential (primary) hypertension"),
    ("E11", "Type 2 diabetes mellitus"),
    ("J18", "Pneumonia, unspecified organism"),
    ("I21", "Acute myocardial infarction"),
    ("N39", "Urinary tract infection"),
    ("K21", "Gastro-esophageal reflux disease"),
    ("F41", "Anxiety disorders"),
    ("M54", "Dorsalgia [back pain]"),
]


def gen_patients(n_patients: int = 500) -> pd.DataFrame:
    """Generate a simple patients dimension table."""
    genders = ["M", "F"]
    birth_starts = datetime(1940, 1, 1)
    birth_ends = datetime(2010, 12, 31)
    delta_days = (birth_ends - birth_starts).days

    rows = []
    for pid in range(1, n_patients + 1):
        g = random.choice(genders)
        bd = birth_starts + timedelta(days=random.randint(0, delta_days))
        rows.append({"patient_id": pid, "gender": g, "birth_date": bd.date()})
    return pd.DataFrame(rows)


def gen_diagnoses() -> pd.DataFrame:
    """Return the fixed diagnosis code table."""
    return pd.DataFrame([{"diagnosis_code": c, "diagnosis_desc": d} for c, d in DIAGNOSES])


def gen_admissions(n_adm: int = 2000, n_patients: int = 500,
                   start_dt: datetime = datetime(2024, 1, 1),
                   end_dt: datetime = datetime(2025, 7, 31)) -> pd.DataFrame:
    """Generate a fact-like admissions table with random LOS and diagnoses."""
    rows = []
    day_span = (end_dt - start_dt).days
    for aid in range(1, n_adm + 1):
        pid = random.randint(1, n_patients)
        hospital_id = random.randint(1, 5)
        room_id = f"W{random.randint(1, 20)}-B{random.randint(1, 4)}"
        admit_day = start_dt + timedelta(days=random.randint(0, day_span))
        # LOS sampled from a gamma distribution -> skewed long tail, realistic for LOS
        los_days = max(1, int(np.random.gamma(shape=2.0, scale=2.0)))
        discharge_day = admit_day + timedelta(days=los_days)
        dx_code, _ = random.choice(DIAGNOSES)

        rows.append({
            "admission_id": aid,
            "patient_id": pid,
            "admit_time": admit_day,
            "discharge_time": discharge_day,
            "primary_diagnosis": dx_code,
            "hospital_id": hospital_id,
            "room_id": room_id
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare CSVs.")
    parser.add_argument("--rows", type=int, default=2000, help="Number of admissions to generate")
    parser.add_argument("--patients", type=int, default=500, help="Number of patients to generate")
    parser.add_argument("--outdir", type=str, default=str(Path(__file__).resolve().parents[1] / "data"),
                        help="Output directory for CSVs")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    patients = gen_patients(args.patients)
    diagnoses = gen_diagnoses()
    admissions = gen_admissions(args.rows, args.patients)

    patients.to_csv(outdir / "patients.csv", index=False)
    diagnoses.to_csv(outdir / "diagnoses.csv", index=False)
    admissions.to_csv(outdir / "admissions.csv", index=False)

    print(f"Wrote: {outdir} (patients.csv, diagnoses.csv, admissions.csv)")


if __name__ == "__main__":
    main()
