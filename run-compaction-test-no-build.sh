#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to run Ozone compaction interruption tests WITHOUT building the project
# Uses pre-built Docker images from Apache Ozone

set -e

echo "=== Ozone Compaction Interruption Test (No Build Required) ==="
echo "This test uses pre-built Docker images - no Maven build needed!"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose > /dev/null 2>&1; then
    echo "❌ Error: Docker Compose is not installed. Please install it and try again."
    exit 1
fi

# Navigate to the compose directory
COMPOSE_DIR="hadoop-ozone/dist/src/main/compose/ozonesecure-ha"
cd "$COMPOSE_DIR"

echo "📁 Working directory: $(pwd)"
echo ""

# Set environment variables for the test
echo "🔧 Setting up environment variables..."
export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:byteman.yaml

# Set default Docker images if not already set
export OZONE_RUNNER_IMAGE=${OZONE_RUNNER_IMAGE:-"apache/ozone-runner"}
export OZONE_RUNNER_VERSION=${OZONE_RUNNER_VERSION:-"latest"}
export HADOOP_IMAGE=${HADOOP_IMAGE:-"apache/hadoop"}
export HADOOP_VERSION=${HADOOP_VERSION:-"3.4.1"}
export OZONE_TESTKRB5_IMAGE=${OZONE_TESTKRB5_IMAGE:-"apache/hadoop-testkrb5"}

echo "🐳 Using Docker images:"
echo "   - Ozone Runner: ${OZONE_RUNNER_IMAGE}:${OZONE_RUNNER_VERSION}"
echo "   - Hadoop: ${HADOOP_IMAGE}:${HADOOP_VERSION}"
echo "   - Test KRB5: ${OZONE_TESTKRB5_IMAGE}"
echo ""

# Source the test library
echo "📚 Loading test library..."
source "../testlib.sh"

# Start the docker environment
echo "🚀 Starting Ozone cluster with Byteman enabled..."
start_docker_env

echo "✅ Cluster started successfully!"
echo ""

# Wait a moment for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Run the compaction interruption test
echo "🧪 Running compaction interruption test..."
execute_robot_test om1 ozone-fi/compaction_interruption.robot

echo ""
echo "🎉 Test completed successfully!"
echo ""
echo "=== Test Summary ==="
echo "✅ Ozone cluster started with pre-built Docker images"
echo "✅ Byteman fault injection enabled"
echo "✅ Compaction interruption tests executed"
echo "✅ Data consistency validated"
echo ""
echo "📊 Test results available in: $(pwd)/result/"
echo ""
echo "🔍 To view logs:"
echo "   docker-compose logs om1"
echo "   docker-compose logs datanode1"
echo ""
echo "🧪 To run additional tests:"
echo "   ./test-byteman.sh"
echo ""
echo "🛑 To stop the cluster:"
echo "   docker-compose down"
echo ""
echo "💡 Tips:"
echo "   - The cluster uses pre-built images, so no compilation needed"
echo "   - Byteman scripts are mounted from your local dev-support/byteman/ directory"
echo "   - All test artifacts are preserved in the result/ directory" 