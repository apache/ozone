#!/bin/bash
# Script to build minimal Ozone components and run compaction tests
# This avoids building the entire project with all its dependencies

set -e

echo "=== Minimal Ozone Build + Compaction Test ==="
echo "Building only essential components for testing..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Function to build specific modules
build_module() {
    local module=$1
    echo "🔨 Building module: $module"
    
    # Try to build with dependency analysis disabled
    mvn clean install -pl "$module" -DskipTests -Dmaven.dependency.analyze.skip=true -q || {
        echo "⚠️  Building $module with dependency analysis failed, trying without..."
        mvn clean install -pl "$module" -DskipTests -q || {
            echo "❌ Failed to build $module"
            return 1
        }
    }
    echo "✅ Built $module successfully"
}

# Build only the essential modules for testing
echo "📦 Building essential modules..."

# Build order matters - build dependencies first
build_module "hadoop-hdds/common" || exit 1
build_module "hadoop-hdds/interface-client" || exit 1
build_module "hadoop-hdds/interface-admin" || exit 1
build_module "hadoop-hdds/interface-server" || exit 1
build_module "hadoop-hdds/client" || exit 1
build_module "hadoop-hdds/erasurecode" || exit 1
build_module "hadoop-hdds/container-service" || exit 1
build_module "hadoop-hdds/server-scm" || exit 1
build_module "hadoop-ozone/common" || exit 1
build_module "hadoop-ozone/client" || exit 1
build_module "hadoop-ozone/ozone-manager" || exit 1
build_module "hadoop-ozone/datanode" || exit 1
build_module "hadoop-ozone/dist" || exit 1

echo ""
echo "✅ All essential modules built successfully!"
echo ""

# Now run the compaction test
echo "🧪 Running compaction interruption test..."

# Navigate to compose directory
cd hadoop-ozone/dist/src/main/compose/ozonesecure-ha

# Set environment variables
export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:byteman.yaml

# Source the test library
source "../testlib.sh"

# Start the docker environment
echo "🚀 Starting Ozone cluster..."
start_docker_env

echo "✅ Cluster started successfully!"
echo ""

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 15

# Run the compaction interruption test
echo "🧪 Running compaction interruption test..."
execute_robot_test om1 ozone-fi/compaction_interruption.robot

echo ""
echo "🎉 Test completed successfully!"
echo ""
echo "=== Summary ==="
echo "✅ Built only essential modules (avoided full project build)"
echo "✅ Started Ozone cluster with Byteman enabled"
echo "✅ Ran compaction interruption tests"
echo "✅ Results available in: $(pwd)/result/"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose down" 