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

# Clean table (Target for update)
PATEK_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.patek"
# FX table (Source of rates)
FX_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.fx_rates"

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

def update_patek_with_eur():
    """
    WORKAROUND FOR FREE TIER:
    Since 'UPDATE' (DML) is forbidden in the sandbox, we use 
    'CREATE OR REPLACE TABLE' (DDL) to overwrite the table 
    with the enriched data.
    """
    print("💶 Enriching Patek table with EUR prices (CTAS Strategy)...")

    # 1. Safety: Drop the column if it already exists to avoid duplication errors
    try:
        sql_drop = f"ALTER TABLE `{PATEK_TABLE}` DROP COLUMN IF EXISTS price_EUR"
        client.query(sql_drop).result()
    except Exception as e:
        print(f"⚠️ Note: Could not drop column (might not exist yet): {e}")

    # 2. Overwrite the table with the new calculation
    # FIX: We use DATE(fx.date) to ensure we compare DATE with DATE
    sql_overwrite = f"""
        CREATE OR REPLACE TABLE `{PATEK_TABLE}` AS
        SELECT 
            p.*,
            (p.price * fx.rate) AS price_EUR
        FROM `{PATEK_TABLE}` p
        LEFT JOIN `{FX_TABLE}` fx
            ON p.currency = fx.base_currency 
            AND DATE(p.life_span_date) = DATE(fx.date)
    """
    
    job = client.query(sql_overwrite)
    job.result() # Wait for completion
    
    print(f"✅ Success! Recreated '{PATEK_TABLE}' with 'price_EUR' column.")

def main():
    print(f"Project: {PROJECT_ID}")
    
    # 1. Get unique dates/currencies from the CLEAN patek table
    query = f"""
    SELECT DISTINCT
        DATE(life_span_date) AS date,
        currency
    FROM `{PATEK_TABLE}`
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
            if i % 10 == 0: time.sleep(0.1) 

        if rate is not None:
            rows.append({
                "date": date_str,
                "base_currency": base,
                "target_currency": TARGET_CCY,
                "rate": rate
            })

    # 3. Upload Rates to BigQuery
    if rows:
        df = pd.DataFrame(rows)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, FX_TABLE, job_config=job_config)
        job.result()
        print(f"✅ Loaded {len(df)} rates to {FX_TABLE}")
        
        # Save CSV for PowerBI (Workaround)
        pd.DataFrame(rows).to_csv("fx_rates.csv", index=False)
        print("✅ Saved fx_rates.csv for PowerBI")
        
        # --- NEW STEP: ENRICH BIGQUERY ---
        update_patek_with_eur()
        
    else:
        print("Warning: No rates collected.")

if __name__ == "__main__":
    main()