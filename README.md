# FairPrice Meiji Milk Tracker

A small Streamlit app for tracking `Meiji Low Fat High Protein Milk (Original 350ml)` across FairPrice stores in Singapore.

It collects live stock data from FairPrice endpoints, stores historical snapshots as CSV files, and visualizes current inventory on a map with history and movement analysis.

## What It Does

- Checks warehouse and per-store stock for the tracked product
- Stores snapshots locally as CSV files
- Shows current store inventory on a map
- Lets you search stores near a postal code
- Tracks warehouse stock and price history over time
- Infers lower-bound sales and restocks between snapshots
- Includes macOS launch and app-bundling scripts

## Project Files

- `app.py` - Streamlit dashboard
- `stock_job.py` - stock collection job
- `fairprice_api.py` - FairPrice API integration
- `data_store.py` - CSV-based persistence helpers
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

## Data Model

This project does not use a database. It persists everything as CSV files.

- `warehouse_history.csv` stores warehouse snapshots
- `store_stock_history.csv` stores per-store snapshots grouped by `batch_id`
- `stores.csv` stores normalized store metadata

In normal local development, files are written under `data/`.

## Notes

- The tracked product SKU and slug are currently hard-coded in `fairprice_api.py`.
- The app is built around a single product workflow, not a general inventory tracker.
- Stock movement analytics are inferred from snapshots, so they represent minimum possible sales/restocks rather than exact transactions.
