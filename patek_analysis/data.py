import os
import pandas as pd
from google.cloud import bigquery

def get_patek_data():
    """
    Queries the Patek Philippe table from BigQuery
    """
    print("⏳ Connecting to BigQuery...")
    
    # 1. Setup Client (Auth is handled by env variable)
    client = bigquery.Client()
    
    # 2. Define Query 
    # Project: projectbdm-487109 | Dataset: patek_data | Table: patek
    query = """
    SELECT *
    FROM `projectbdm-487109.patek_data.patek_raw`
    """
    
    # 3. Run Query
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    
    print(f"✅ Data loaded! Shape: {df.shape}")

    # ### NEW: CLEANING STEP ###
    # We want the data to be cleaned automatically the moment the user download it
    # This removes any row where 'price' is empty (NaN/None).
    initial_count = len(df)
    df = df.dropna(subset=['price'])
    final_count = len(df)
    print(f"🧹 Cleaned Data: Removed {initial_count - final_count} rows with empty prices.")

    print(df.head())

    # Ensure output data directory exists
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    # Save the csv to data/ folder
    output_file = os.path.join(output_dir, "patek_philippe_data_cleaned.csv")
    df.to_csv(output_file, index=False)


    print(f"✅ Success! Data saved to {output_file}")
    return df

if __name__ == '__main__':
    get_patek_data()