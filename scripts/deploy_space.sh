#!/usr/bin/env bash
# Create (if needed) and update the public static Space from app/ (stlite, D-033). Needs `hf auth login` first.
set -euo pipefail
cd "$(dirname "$0")/.."
HF="uvx --from huggingface_hub==2.4.0 hf"
NAME="${1:-opening-finder}"
USER_ID="$($HF auth whoami --format json 2>/dev/null \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["user"])' 2>/dev/null || true)"
[ -n "$USER_ID" ] || { echo "not logged in: run hf auth login" >&2; exit 1; }
REPO="$USER_ID/$NAME"
$HF repos create "$REPO" --type space --sdk static --public --exist-ok
$HF upload "$REPO" app . --repo-type space \
  --exclude "__pycache__/*" --exclude "*.pyc" --exclude ".DS_Store" \
  --commit-message "Deploy $(git rev-parse --short HEAD)"
echo "https://huggingface.co/spaces/$REPO"
