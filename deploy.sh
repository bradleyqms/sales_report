#!/bin/bash
# Kudu custom deployment script for qms-dispatch-reports Function App.
# Runs on the Azure SCM site (same Linux/Python 3.11 env as the function worker).
# DEPLOYMENT_TARGET is set by Kudu to /home/site/wwwroot.
set -euo pipefail

TARGET="${DEPLOYMENT_TARGET:-/home/site/wwwroot}"

echo ">>> Deploying to: $TARGET"

# ── Function App host files ───────────────────────────────────────────────────
cp azure_functions/host.json        "$TARGET/"
cp azure_functions/requirements.txt "$TARGET/"

# ── Timer-triggered functions + shared submodules ─────────────────────────────
cp -r azure_functions/dispatch_reports/    "$TARGET/dispatch_reports/"
cp -r azure_functions/core_market_reports/ "$TARGET/core_market_reports/"

# ── src/ — full_report.py and all report/SharePoint modules ──────────────────
cp -r src/ "$TARGET/src/"

# ── Placeholder .env so load_dotenv() doesn't crash ──────────────────────────
touch "$TARGET/.env"

# ── Install Python dependencies ───────────────────────────────────────────────
# Runs natively in the Kudu SCM environment (Python 3.11, Linux x86_64),
# so binary wheels (pandas, reportlab, etc.) are correct for the function worker.
echo ">>> Installing Python dependencies..."
pip install -r "$TARGET/requirements.txt" \
    --target="$TARGET/.python_packages/lib/site-packages" \
    --quiet --no-cache-dir

echo ">>> Deployment complete."
