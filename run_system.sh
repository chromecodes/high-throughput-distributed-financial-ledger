#!/usr/bin/env bash

# Exit immediately if any individual command fails
set -e

echo "🚀 Step 1: Booting up Docker Compose Infrastructure..."
docker-compose up -d

echo "⏳ Step 2: Waiting for databases and brokers to become healthy..."
# Loops until the postgres container returns a healthy status flag
until [ "$(docker inspect --format='{{.State.Health.Status}}' ledger-postgres)" == "healthy" ]; do
    echo "  -> Waiting for PostgreSQL storage engine..."
    sleep 2
done
echo "✅ Infrastructure components are fully operational!"

echo "📦 Step 3: Activating project virtual environment..."
if [ ! -d "venv" ]; then
    echo "❌ Error: Virtual environment 'venv' not found. Please create it first."
    exit 1
fi
source venv/bin/activate

echo "📝 Step 4: Ensuring database schemas are initialized..."
python schema_init.py

# Deactivate 'set -e' because background execution hooks return non-zero codes during threading
set +e

echo "🔥 Step 5: Launching background data pipeline worker processes..."

# Launch the FastAPI Web Application Server
echo "  -> Starting FastAPI Ingestion Engine on Port 8000..."
uvicorn main:app --port 8000 > fastapi.log 2>&1 &
FASTAPI_PID=$!

# Launch the PostgreSQL Outbox Worker Daemon
echo "  -> Starting Outbox Synchronization Daemon..."
python outbox_worker.py > outbox_worker.log 2>&1 &
OUTBOX_PID=$!

# Launch the ClickHouse Big Data Warehouse Sync Consumer
echo "  -> Starting ClickHouse Big Data Analytical Consumer..."
python consumer.py > consumer.log 2>&1 &
CONSUMER_PID=$!

echo "================================================================="
echo " 🎉 SYSTEM IS TOTALLY ACTIVE!"
echo "================================================================="
echo "📊 Monitoring Log Files Created in Workspace:"
echo "   - FastAPI Log:       tail -f fastapi.log"
echo "   - Outbox Log:        tail -f outbox_worker.log"
echo "   - Analytics Log:     tail -f consumer.log"
echo ""
echo "💡 To shut down all services cleanly, execute: kill $FASTAPI_PID $OUTBOX_PID $CONSUMER_PID"
echo "================================================================="

# Keep the shell execution active so the user can easily monitor or terminate later
wait
