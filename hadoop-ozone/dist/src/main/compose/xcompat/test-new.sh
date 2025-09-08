#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#suite:compat-new

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=hadoop-ozone/dist/src/main/compose/xcompat/lib.sh
source "${COMPOSE_DIR}/lib.sh"

# current cluster with various clients
COMPOSE_FILE=new-cluster.yaml:clients.yaml cluster_version=${current_version} test_cross_compatibility ${old_versions} ${current_version}

# Run checkpoint compatibility tests specifically for 2.0 client
echo ""
echo "=========================================="
echo "Running checkpoint compatibility tests with 2.0 client"
echo "=========================================="

COMPOSE_FILE=new-cluster.yaml:clients.yaml

echo "Starting current cluster for checkpoint testing..."
OZONE_KEEP_RESULTS=true start_docker_env 5

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

# Basic initialization similar to _init
container=scm
execute_command_in_container ${container} kinit -k -t /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM
execute_command_in_container ${container} ozone freon ockg -n1 -t1 -p warmup

# Test 2.0 client against current cluster
client_version="2.0.0" cluster_version=${current_version} client _test_checkpoint_compatibility

KEEP_RUNNING=false stop_docker_env
