#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  📈  Finans Takip — Borsa MCP"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Bağımlılıklar yüklü değilse yükle
if ! python3 -c "import fastapi" 2>/dev/null; then
  echo "→ Bağımlılıklar yükleniyor…"
  pip3 install -r requirements.txt -q
fi

echo "→ Uygulama başlatılıyor: http://localhost:8000"
echo "   Durdurmak için Ctrl+C"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
