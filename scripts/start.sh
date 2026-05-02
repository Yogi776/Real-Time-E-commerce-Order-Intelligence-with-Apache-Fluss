#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo " E-commerce Order Intelligence Platform"
echo " Starting services..."
echo "=========================================="
echo ""

# Check prerequisites
if ! command -v docker &>/dev/null; then
    echo "[ERROR] Docker is not installed. Install from https://docs.docker.com/get-docker/"
    exit 1
fi

if ! docker compose version &>/dev/null; then
    echo "[ERROR] Docker Compose plugin not found. Install from https://docs.docker.com/compose/install/"
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo "[ERROR] Docker daemon is not running. Start Docker Desktop or the Docker service."
    exit 1
fi

echo "[OK] Docker and Docker Compose detected"
echo ""

# Start services
echo "[INFO] Starting all services (this may take 1-2 minutes on first run)..."
docker compose up -d

echo ""
echo "[INFO] Waiting for services to become healthy..."
sleep 10

# Check service status
echo ""
echo "=========================================="
echo " Service Status"
echo "=========================================="
docker compose ps
echo ""

echo "=========================================="
echo " Access Points"
echo "=========================================="
echo "  Flink Web UI:    http://localhost:8083"
echo "  ZooKeeper:       localhost:2181"
echo "  Fluss Coord:     localhost:9123"
echo "  Fluss Tablet:    localhost:9124"
echo ""
echo "=========================================="
echo " Next Steps"
echo "=========================================="
echo "  1. Open the Flink SQL Client:"
echo "     ./scripts/open-sql-client.sh"
echo ""
echo "  2. Or run the full demo:"
echo "     ./scripts/run-demo.sh"
echo ""
echo "  3. Stop everything when done:"
echo "     ./scripts/stop.sh"
echo ""
