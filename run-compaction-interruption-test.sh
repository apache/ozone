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

# Script to run Ozone compaction interruption acceptance tests
# This demonstrates how to interrupt and restart RocksDB compaction flow

set -e

echo "=== Ozone Compaction Interruption Acceptance Test ==="
echo "This test demonstrates fault injection during RocksDB compaction"
echo ""

# Navigate to the compose directory
COMPOSE_DIR="hadoop-ozone/dist/src/main/compose/ozonesecure-ha"
cd "$COMPOSE_DIR"

echo "1. Setting up environment variables..."
export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:byteman.yaml

echo "2. Starting Ozone cluster with Byteman enabled..."
# Source the test library
source "../testlib.sh"

# Start the docker environment
start_docker_env

echo "3. Running compaction interruption test..."
# Run the compaction interruption test
execute_robot_test om1 ozone-fi/compaction_interruption.robot

echo "4. Test completed successfully!"
echo ""
echo "=== Test Summary ==="
echo "The test demonstrated:"
echo "- Injecting faults during RocksDB compaction operations"
echo "- Testing compaction recovery after interruptions"
echo "- Validating data consistency after compaction restarts"
echo ""
echo "Check the test results in: $COMPOSE_DIR/result/"
echo ""
echo "To run additional fault injection tests:"
echo "  ./test-byteman.sh"
echo ""
echo "To stop the cluster:"
echo "  docker-compose down" 