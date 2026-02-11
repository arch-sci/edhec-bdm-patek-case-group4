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
    FROM `projectbdm-487109.patek_data.patek`
    """
    
    # 3. Run Query
    query_job = client.query(query)
    result = query_job.result()
    df = result.to_dataframe()
    
    print(f"✅ Data loaded! Shape: {df.shape}")
    print(df.head())
    
    # Save to CSV
    output_file = "patek_philippe_data.csv"
    df.to_csv(output_file, index=False)
    
    print(f"✅ Success! Data saved to {output_file}")
    return df

if __name__ == '__main__':
    get_patek_data()