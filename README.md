# Patek Philippe Data Engineering Pipeline

A containerized data pipeline that ingests luxury watch data, enriches it with historical exchange rates, and predicts prices using Machine Learning.

## 🏗 Architecture
This project uses a "Lakehouse" approach with **Google BigQuery** as the central source of truth.

- **Storage Layers:**
  - `patek_raw`: Raw data backup (Ingestion layer).
  - `patek`: Production data (Cleaned & Enriched layer).
  - `fx_rates`: Historical currency exchange rates.

- **The Pipeline (Dockerized):**
  1.  **Extraction & Cleaning:** `data.py` pulls data from BigQuery, sanitizes it, and prepares it for analysis.
  2.  **Enrichment (API):** `fx_rates.py` fetches historical rates from the *Frankfurter API* for every specific date/currency pair in the dataset.
  3.  **Transformation (SQL):** A custom "CTAS" (Create Table As Select) strategy is used to join the data and calculate `price_EUR` directly inside BigQuery, bypassing Free Tier DML restrictions.
  4.  **Modeling (ML):** `model.py` trains a Random Forest Regressor to predict watch values based on reference, collection, and currency.

## 🚀 How to Run
Everything runs inside a Docker container for reproducibility.

### 1. Run the Full Pipeline
Fetches data, updates FX rates, trains the model, and outputs CSVs.
```bash
docker compose up --build
```

### 2. Open Jupyter Notebooks
To explore the data visually:
```bash
docker compose run --service-ports app make notebook
```
Then open `localhost:8888` in your browser.

## 🛠 Tech Stack
- **Language:** Python 3.10
- **Container:** Docker & Docker Compose
- **Cloud:** Google BigQuery
- **Libraries:** Pandas, Scikit-Learn, Google Cloud SDK
```