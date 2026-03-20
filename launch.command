#!/bin/bash
# FairPrice Meiji Milk Tracker - Double-click to launch!
# Close this terminal window to stop the dashboard.

cd "$(dirname "$0")"

echo "====================================="
echo "  Meiji Milk Stock Tracker"
echo "====================================="
echo ""

# Check for Python 3
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed."
    echo "Please install it from https://www.python.org/downloads/"
    echo ""
    read -p "Press Enter to close..."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "First-time setup: creating virtual environment..."
    python3 -m venv venv
    echo ""
fi

# Activate venv
source venv/bin/activate

# Install dependencies if needed
if ! python3 -c "import streamlit" 2>/dev/null; then
    echo "Installing dependencies (one-time only)..."
    pip install -q -r requirements.txt
    echo "Done!"
    echo ""
fi

# Run a quick stock check if no data exists
if [ ! -f "data/store_stock_history.csv" ]; then
    echo "First run - collecting stock data (~40 seconds)..."
    python3 stock_job.py
    echo ""
fi

echo "Starting dashboard..."
echo "Opening browser at http://localhost:8501"
echo ""
echo ">>> Close this window to stop the dashboard <<<"
echo ""

# Open browser after a short delay
(sleep 2 && open http://localhost:8501) &

# Run streamlit
python3 -m streamlit run app.py --server.headless true
