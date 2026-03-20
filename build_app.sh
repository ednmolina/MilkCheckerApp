#!/bin/bash
# Build a self-contained Mac .app with bundled Python + dependencies
set -e

APP_NAME="Meiji Milk Tracker"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$PROJECT_DIR/build"
APP_BUNDLE="$BUILD_DIR/${APP_NAME}.app"
CONTENTS="$APP_BUNDLE/Contents"
RESOURCES="$CONTENTS/Resources"
MACOS="$CONTENTS/MacOS"

echo "====================================="
echo "  Building ${APP_NAME}.app"
echo "====================================="

# Clean previous build
rm -rf "$BUILD_DIR"
mkdir -p "$MACOS" "$RESOURCES/app" "$RESOURCES/venv"

echo ""
echo "[1/5] Copying application code..."
for f in app.py data_store.py fairprice_api.py stock_job.py stores_with_coords.json requirements.txt; do
    cp "$PROJECT_DIR/$f" "$RESOURCES/app/"
done
# Copy data directory if it exists
if [ -d "$PROJECT_DIR/data" ]; then
    cp -r "$PROJECT_DIR/data" "$RESOURCES/app/data"
fi

echo "[2/5] Creating bundled Python virtual environment..."
python3 -m venv "$RESOURCES/venv"

echo "[3/5] Installing dependencies into bundle..."
"$RESOURCES/venv/bin/pip" install -q --upgrade pip
"$RESOURCES/venv/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt"

echo "[4/5] Creating app launcher..."
cat > "$MACOS/launcher" << 'LAUNCHER_EOF'
#!/bin/bash
# Self-contained Meiji Milk Tracker launcher
APP_CONTENTS="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR="$APP_CONTENTS/Resources/app"
VENV_DIR="$APP_CONTENTS/Resources/venv"
PYTHON="$VENV_DIR/bin/python3"
LOG_FILE="$APP_DIR/launcher.log"

# Activate the bundled venv
export PATH="$VENV_DIR/bin:$PATH"
export VIRTUAL_ENV="$VENV_DIR"
export MEIJI_APP_BUNDLE=1

cd "$APP_DIR"

# Run stock check if no data exists
DATA_CHECK="$HOME/Documents/Meiji Milk Tracker Data/store_stock_history.csv"
if [ ! -f "$DATA_CHECK" ]; then
    osascript -e 'display notification "Collecting stock data for the first time (~40s)..." with title "Meiji Milk Tracker" subtitle "First Run Setup"'
    "$PYTHON" stock_job.py >> "$LOG_FILE" 2>&1
fi

# Open browser after delay
(sleep 3 && open http://localhost:8501) &

# Show notification
osascript -e 'display notification "Dashboard is starting at localhost:8501" with title "Meiji Milk Tracker" subtitle "Starting..."'

# Run Streamlit using the bundled Python
exec "$PYTHON" -m streamlit run app.py \
    --server.headless true \
    --server.port 8501 \
    --browser.gatherUsageStats false \
    --global.developmentMode false \
    >> "$LOG_FILE" 2>&1
LAUNCHER_EOF
chmod +x "$MACOS/launcher"

echo "[5/5] Creating app metadata..."
cat > "$CONTENTS/Info.plist" << PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>launcher</string>
    <key>CFBundleName</key>
    <string>Meiji Milk Tracker</string>
    <key>CFBundleDisplayName</key>
    <string>Meiji Milk Tracker</string>
    <key>CFBundleIdentifier</key>
    <string>com.fairprice.meiji-tracker</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.15</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>LSUIElement</key>
    <false/>
</dict>
</plist>
PLIST_EOF

echo ""
echo "====================================="
echo "  Build complete!"
echo "====================================="
APP_SIZE=$(du -sh "$APP_BUNDLE" | cut -f1)
echo "  App: $APP_BUNDLE"
echo "  Size: $APP_SIZE"
echo ""
echo "  To share: zip it up and send!"
echo "  zip -r \"${APP_NAME}.zip\" \"build/${APP_NAME}.app\""
echo "====================================="
