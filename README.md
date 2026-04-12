# FairPrice Meiji Milk Tracker

A small Streamlit app for tracking `Meiji Low Fat High Protein Milk (Chocolate 350ml)` across FairPrice stores in Singapore.

It collects live stock data from FairPrice endpoints, stores historical snapshots in local CSV files or Google Sheets, and visualizes current inventory on a map with history and movement analysis.

## What It Does

- Checks warehouse and per-store stock for the tracked product
- Stores snapshots locally as CSV files or in Google Sheets
- Shows current store inventory on a map
- Lets you search stores near a postal code
- Tracks warehouse stock and price history over time
- Infers lower-bound sales and restocks between snapshots
- Includes macOS launch and app-bundling scripts

## Project Files

- `app.py` - Streamlit dashboard
- `stock_job.py` - stock collection job
- `fairprice_api.py` - FairPrice API integration
- `data_store.py` - persistence helpers for local CSV or Google Sheets
- `launch.command` - double-click launcher for macOS
- `build_app.sh` - creates a bundled macOS `.app`
- `stores_with_coords.json` - store metadata with coordinates
- `data/` - local CSV history files

## Requirements

- Python 3.10+
- macOS if you want to use `launch.command` or build the `.app`

Install Python dependencies with:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running The Tracker

### 1. Collect stock data

```bash
python3 stock_job.py
```

This creates or updates:

- `data/store_stock_history.csv`
- `data/warehouse_history.csv`
- `data/stores.csv`

### 2. Start the dashboard

```bash
python3 -m streamlit run app.py
```

Then open `http://localhost:8501`.

## macOS Shortcut

You can also launch the app with:

```bash
./launch.command
```

On first run it will:

- create a local virtual environment if needed
- install dependencies if they are missing
- run an initial stock check if no history exists
- open the Streamlit dashboard in your browser

## Building A macOS App Bundle

To build the standalone app bundle:

```bash
./build_app.sh
```

The generated app is written to:

```text
build/Meiji Milk Tracker.app
```

When the bundled app runs, data is stored in:

```text
~/Documents/Meiji Milk Tracker Data
```

## Deploying To Streamlit Community Cloud With Google Sheets

This app can run on Streamlit Community Cloud while using Google Sheets as the durable data store.

### 1. Create one Google spreadsheet with 3 tabs

Create these worksheet tabs exactly:

- `store_stock_history`
- `warehouse_history`
- `stores`

### 2. Import your current local data into those tabs

Take the files from your local `data/` folder and import them like this:

- `data/store_stock_history.csv` -> worksheet tab `store_stock_history`
- `data/warehouse_history.csv` -> worksheet tab `warehouse_history`
- `data/stores.csv` -> worksheet tab `stores`

Each CSV should be imported into its own tab, with the header row preserved.

### 3. Create a Google service account

Follow the Streamlit private Google Sheets tutorial:

https://docs.streamlit.io/develop/tutorials/databases/private-gsheet

After you create the service account JSON key:

- share the spreadsheet with the service account's `client_email`
- give it `Editor` access

### 4. Add secrets for local dev or Streamlit Cloud

Use `.streamlit/secrets.example.toml` in this repo as your template.

For local development:

- copy it to `.streamlit/secrets.toml`
- fill in the real spreadsheet URL and service account fields

For Streamlit Community Cloud:

- open your app settings
- go to `Secrets`
- paste the same TOML content there

The app will automatically switch from local CSV storage to Google Sheets when those secrets are present.

### 5. Deploy on Streamlit Community Cloud

- Push the repo to GitHub
- In Streamlit Community Cloud, create a new app from this repo
- Set the main file path to `app.py`
- Add the Google Sheets secrets
- Deploy

### Important behavior

- The app and `Run Stock Check Now` button will write new snapshots into Google Sheets when the secrets are configured.
- Without Google Sheets secrets, the app falls back to local CSV files.

## Notes

- The tracked product SKU and slug are currently hard-coded in `fairprice_api.py`.
- The app is built around a single product workflow, not a general inventory tracker.
- Stock movement analytics are inferred from snapshots, so they represent minimum possible sales/restocks rather than exact transactions.
