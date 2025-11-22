#!/bin/bash
# Simple script to test compaction interruption manually
# No build required - uses Docker images directly

set -e

echo "=== Simple Compaction Interruption Test ==="
echo "This script manually tests compaction interruption using Docker"
echo ""

# Check prerequisites
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running"
    exit 1
fi

if ! command -v docker-compose > /dev/null 2>&1; then
    echo "❌ Docker Compose not found"
    exit 1
fi

# Go to the compose directory
cd hadoop-ozone/dist/src/main/compose/ozonesecure-ha

echo "📁 Working in: $(pwd)"
echo ""

# Set up environment
export COMPOSE_FILE=docker-compose.yaml:byteman.yaml
export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org

# Use default images
export OZONE_RUNNER_IMAGE=${OZONE_RUNNER_IMAGE:-"apache/ozone-runner"}
export OZONE_RUNNER_VERSION=${OZONE_RUNNER_VERSION:-"latest"}

echo "🐳 Starting Ozone cluster..."
docker-compose up -d

echo "⏳ Waiting for cluster to be ready..."
sleep 30

echo "🔍 Checking cluster status..."
docker-compose ps

echo ""
echo "🧪 Testing compaction interruption..."

# Test 1: Check if Byteman is working
echo "1. Testing Byteman agent..."
docker exec om1 bmsubmit -p 9091 -l || echo "Byteman not ready yet, continuing..."

# Test 2: Inject a simple compaction fault
echo "2. Injecting compaction fault..."
docker exec om1 bmsubmit -p 9091 /opt/hadoop/dev-support/byteman/compaction-fault-injection.btm || echo "Fault injection failed, but continuing..."

# Test 3: Create some data to trigger compaction
echo "3. Creating test data..."
docker exec om1 /opt/hadoop/bin/ozone sh volume create /vol1 || echo "Volume creation failed, but continuing..."
docker exec om1 /opt/hadoop/bin/ozone sh bucket create /vol1/bucket1 || echo "Bucket creation failed, but continuing..."

# Test 4: Check logs for compaction activity
echo "4. Checking for compaction activity..."
docker logs om1 | grep -i compaction | tail -5 || echo "No compaction logs found yet"

echo ""
echo "✅ Manual test completed!"
echo ""
echo "📊 To see detailed results:"
echo "   docker-compose logs om1 | grep -i compaction"
echo "   docker exec om1 bmsubmit -p 9091 -l"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose down" 