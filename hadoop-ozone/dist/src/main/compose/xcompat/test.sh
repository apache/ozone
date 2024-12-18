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

#suite:compat

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR
basename=$(basename ${COMPOSE_DIR})

current_version="${ozone.version}"
old_versions="1.0.0 1.1.0 1.2.1 1.3.0 1.4.0" # container is needed for each version in clients.yaml

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

test_ec_cross_compatibility() {
  echo "Running Erasure Coded storage backward compatibility tests."
  # local cluster_versions_with_ec="1.3.0 1.4.0 ${current_version}"
  local cluster_versions_with_ec="${current_version}" # until HDDS-11334
  local non_ec_client_versions="1.0.0 1.1.0 1.2.1"

  for cluster_version in ${cluster_versions_with_ec}; do
    export COMPOSE_FILE=new-cluster.yaml:clients.yaml cluster_version=${cluster_version}
    OZONE_KEEP_RESULTS=true start_docker_env 5

    echo -n "Generating data locally...   "
    dd if=/dev/urandom of=/tmp/1mb bs=1048576 count=1 >/dev/null 2>&1
    dd if=/dev/urandom of=/tmp/2mb bs=1048576 count=2 >/dev/null 2>&1
    dd if=/dev/urandom of=/tmp/3mb bs=1048576 count=3 >/dev/null 2>&1
    echo "done"
    echo -n "Copy data into client containers...   "
    for container in $(docker ps --format '{{.Names}}' | grep client); do
      docker cp /tmp/1mb ${container}:/tmp/1mb
      docker cp /tmp/2mb ${container}:/tmp/2mb
      docker cp /tmp/3mb ${container}:/tmp/3mb
    done
    echo "done"
    rm -f /tmp/1mb /tmp/2mb /tmp/3mb


    local prefix=$(LC_CTYPE=C tr -dc '[:alnum:]' < /dev/urandom | head -c 5 | tr '[:upper:]' '[:lower:]')
    OZONE_DIR=/opt/hadoop
    execute_robot_test new_client --include setup-ec-data -N "xcompat-cluster-${cluster_version}-setup-data" -v prefix:"${prefix}" ec/backward-compat.robot
     OZONE_DIR=/opt/ozone

    for client_version in ${non_ec_client_versions}; do
      client="old_client_${client_version//./_}"
      unset OUTPUT_PATH
      execute_robot_test "${client}" --include test-ec-compat -N "xcompat-cluster-${cluster_version}-client-${client_version}-read-${cluster_version}" -v prefix:"${prefix}" ec/backward-compat.robot
    done

    KEEP_RUNNING=false stop_docker_env
  done
}

create_results_dir

# current cluster with various clients
COMPOSE_FILE=new-cluster.yaml:clients.yaml cluster_version=${current_version} test_cross_compatibility ${old_versions}

# old cluster with clients: same version and current version
for cluster_version in ${old_versions}; do
  export OZONE_VERSION=${cluster_version}
  COMPOSE_FILE=old-cluster.yaml:clients.yaml test_cross_compatibility ${cluster_version}
done

test_ec_cross_compatibility
