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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR
basename=$(basename ${COMPOSE_DIR})

current_version=1.3.0
old_versions="1.0.0 1.1.0 1.2.1" # container is needed for each version in clients.yaml

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

old_client() {
  OZONE_DIR=/opt/ozone
  container=${client}
  "$@"
}

new_client() {
  OZONE_DIR=/opt/hadoop
  container=new_client
  client_version=${current_version}
  "$@"
}

_init() {
  execute_command_in_container ${container} ozone freon ockg -n1 -t1 -p warmup
}

_write() {
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-write" -v SUFFIX:${client_version} compatibility/write.robot
}

_read() {
  local data_version="$1"
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-read-${data_version}" -v SUFFIX:${data_version} compatibility/read.robot
}

test_cross_compatibility() {
  echo "Starting cluster with COMPOSE_FILE=${COMPOSE_FILE}"

  OZONE_KEEP_RESULTS=true start_docker_env

  execute_command_in_container scm ozone freon ockg -n1 -t1 -p warmup
  new_client _write
  new_client _read ${current_version}

  for client_version in "$@"; do
    client="old_client_${client_version//./_}"

    old_client _write
    old_client _read ${client_version}

    old_client _read ${current_version}
    new_client _read ${client_version}
  done

  KEEP_RUNNING=false stop_docker_env
}

create_results_dir

# current cluster with various clients
COMPOSE_FILE=new-cluster.yaml:clients.yaml cluster_version=${current_version} test_cross_compatibility ${old_versions}

# old cluster with clients: same version and current version
for cluster_version in ${old_versions}; do
  export OZONE_VERSION=${cluster_version}
  COMPOSE_FILE=old-cluster.yaml:clients.yaml test_cross_compatibility ${cluster_version}
done

generate_report
