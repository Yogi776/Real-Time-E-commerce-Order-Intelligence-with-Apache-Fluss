#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo " Stopping E-commerce Intelligence Platform"
echo "=========================================="
echo ""

if [ "$1" = "--clean" ]; then
    echo "[INFO] Stopping services and removing volumes..."
    docker compose down -v
    echo "[OK] All services stopped and volumes removed."
else
    echo "[INFO] Stopping services (volumes preserved)..."
    docker compose down
    echo "[OK] All services stopped."
    echo ""
    echo "[TIP] To also remove data volumes, run:"
    echo "  ./scripts/stop.sh --clean"
fi
