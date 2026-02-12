import os
import time
import requests
import pandas as pd
from google.cloud import bigquery

# --- CONFIG ---
UNSUPPORTED = {"TWD"}
TARGET_CCY = "EUR"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "patek_data").strip()

# Construct table names dynamically or use defaults
SOURCE_TABLE = os.getenv("SOURCE_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.patek").strip()
DEST_TABLE = os.getenv("DEST_TABLE", f"{PROJECT_ID}.{BQ_DATASET}.fx_rates").strip()

if not PROJECT_ID:
    raise ValueError("Missing env var GCP_PROJECT_ID")

client = bigquery.Client(project=PROJECT_ID)

def fx_rate(date: str, base: str, target: str = "EUR") -> float | None:
    """Fetch historical FX rate from Frankfurter API."""
    url = f"https://api.frankfurter.app/{date}"
    params = {"from": base, "to": target}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("rates", {}).get(target)
    except Exception:
        return None

def main():
    print(f"Project: {PROJECT_ID}")
    print(f"Reading from: {SOURCE_TABLE}")
    print(f"Writing to:   {DEST_TABLE}")

    # 1. Get unique dates and currencies from BigQuery
    query = f"""
    SELECT DISTINCT
        DATE(life_span_date) AS date,
        currency
    FROM `{SOURCE_TABLE}`
    WHERE life_span_date IS NOT NULL AND currency IS NOT NULL
    """
    pairs = client.query(query).to_dataframe()
    print(f"Pairs to process: {len(pairs)}")

    if pairs.empty:
        print("No data found.")
        return

    # 2. Fetch FX Rates
    rows = []
    for i, row in pairs.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        base = row["currency"]

        if base in UNSUPPORTED:
            rate = None
        elif base == TARGET_CCY:
            rate = 1.0
        else:
            rate = fx_rate(date_str, base, TARGET_CCY)
            if i % 10 == 0: time.sleep(0.1) # Be nice to the API

        if rate is not None:
            rows.append({
                "date": date_str,
                "base_currency": base,
                "target_currency": TARGET_CCY,
                "rate": rate
            })

    # 3. Upload to BigQuery
    if rows:
        df = pd.DataFrame(rows)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, DEST_TABLE, job_config=job_config)
        job.result()
        print(f"✅ Success! Loaded {len(df)} rows to {DEST_TABLE}")
    else:
        print("Warning: No rates collected.")

if __name__ == "__main__":
    main()