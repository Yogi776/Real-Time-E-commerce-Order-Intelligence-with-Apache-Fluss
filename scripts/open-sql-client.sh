#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo " Opening Flink SQL Client"
echo "=========================================="
echo ""
echo "You will be dropped into an interactive SQL shell."
echo "Type 'quit;' or press Ctrl+D to exit."
echo ""

docker compose run --rm sql-client
