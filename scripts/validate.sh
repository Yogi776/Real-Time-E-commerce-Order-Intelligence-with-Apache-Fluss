#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo " Repository Validation"
echo "=========================================="
echo ""

ERRORS=0

check_file() {
    if [ -f "$1" ]; then
        echo "  [OK] $1"
    else
        echo "  [MISSING] $1"
        ERRORS=$((ERRORS + 1))
    fi
}

check_dir() {
    if [ -d "$1" ]; then
        echo "  [OK] $1/"
    else
        echo "  [MISSING] $1/"
        ERRORS=$((ERRORS + 1))
    fi
}

check_executable() {
    if [ -x "$1" ]; then
        echo "  [OK] $1 (executable)"
    elif [ -f "$1" ]; then
        echo "  [WARN] $1 exists but not executable. Run: chmod +x $1"
        ERRORS=$((ERRORS + 1))
    else
        echo "  [MISSING] $1"
        ERRORS=$((ERRORS + 1))
    fi
}

echo "--- Core Files ---"
check_file "README.md"
check_file "docker-compose.yml"
check_file ".gitignore"
echo ""

echo "--- SQL Files ---"
check_file "sql/01_create_catalog.sql"
check_file "sql/02_create_tables.sql"
check_file "sql/03_seed_data.sql"
check_file "sql/04_generate_orders.sql"
check_file "sql/05_enrich_orders.sql"
check_file "sql/06_revenue_aggregates.sql"
check_file "sql/07_large_volume_simulation.sql"
check_file "sql/08_anomaly_detection.sql"
check_file "sql/09_demo_queries.sql"
echo ""

echo "--- Data Generator ---"
check_file "datagen/generate_seed_data.py"
echo ""

echo "--- Scripts ---"
check_executable "scripts/start.sh"
check_executable "scripts/stop.sh"
check_executable "scripts/open-sql-client.sh"
check_executable "scripts/run-demo.sh"
check_executable "scripts/validate.sh"
check_executable "scripts/load-test-notes.sh"
echo ""

echo "--- Directories ---"
check_dir "sql"
check_dir "scripts"
check_dir "docs"
check_dir "datagen"
check_dir "dashboard"
echo ""

echo "--- Documentation ---"
check_file "docs/architecture.md"
check_file "docs/solution-architecture.md"
check_file "docs/system-design.md"
check_file "docs/data-model.md"
check_file "docs/large-volume-handling.md"
check_file "docs/performance-optimization.md"
check_file "docs/reliability-and-fault-tolerance.md"
check_file "docs/observability.md"
check_file "docs/fluss-vs-current-architecture.md"
check_file "docs/phase-2-lakehouse-tiering.md"
check_file "docs/production-readiness-checklist.md"
echo ""

echo "--- Docker Compose Validation ---"
if docker compose config --quiet 2>/dev/null; then
    echo "  [OK] docker-compose.yml is valid"
else
    echo "  [WARN] Could not validate docker-compose.yml (Docker may not be running)"
fi
echo ""

echo "=========================================="
if [ $ERRORS -eq 0 ]; then
    echo " All checks passed!"
else
    echo " $ERRORS issue(s) found. See above."
fi
echo "=========================================="
