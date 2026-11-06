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

client() {
  if [[ "${client_version}" == "${current_version}" ]]; then
    OZONE_DIR=/opt/hadoop
    container=new_client
  else
    OZONE_DIR=/opt/ozone
    container="old_client_${client_version//./_}"
  fi

  "$@"
}

_kinit() {
  execute_command_in_container ${container} kinit -k -t /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM
}

_init() {
  container=scm
  _kinit
  execute_command_in_container ${container} ozone freon ockg -n1 -t1 -p warmup
}

_write() {
  _kinit
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-write" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v TEST_DATA_DIR:/testdata \
    compatibility/write.robot
}

_read() {
  _kinit
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-read-${data_version}" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v DATA_VERSION:${data_version} \
    -v TEST_DATA_DIR:/testdata \
    compatibility/read.robot
}

_test_checkpoint_compatibility() {
  _kinit
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-checkpoint" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v TEST_DATA_DIR:/testdata \
    compatibility/checkpoint.robot
}

# Generates version combinations for read tests in the form of
#   reader=writer
# printing to stdout.
#
# A combination is tested if it is related to the current version in any way:
# - cluster running
# - client who wrote the data
# - client to read the data
get_read_version_combinations() {
  [[ -n "${cluster_version}" ]] || return 1
  [[ -n "${current_version}" ]] || return 2
  [[ -n "${old_versions}" ]] || return 3

  local all_versions="$old_versions $current_version"
  local reader writer
  for reader in ${all_versions}; do
    for writer in ${all_versions}; do
      if [[ "${cluster_version}" == "${current_version}" ]] \
          || [[ "${reader}" == "${current_version}" ]] \
          || [[ "${writer}" == "${current_version}" ]]; then
        echo "$reader=$writer"
      fi
    done
  done | sort --reverse --unique
}

test_cross_compatibility() {
  local read_combinations=$(get_read_version_combinations)
  if [[ -z "$read_combinations" ]]; then
    echo "No read test combinations found" >&2
    return 1
  fi

  echo "Starting ${cluster_version} cluster with COMPOSE_FILE=${COMPOSE_FILE}"

  OZONE_KEEP_RESULTS=true start_docker_env 5

  execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

  _init

  # first write with client matching cluster version
  client_version="${cluster_version}" client _write

  for client_version in ${old_versions} ${current_version}; do
    # skip write, since already done
    if [[ "${client_version}" == "${cluster_version}" ]]; then
      continue
    fi
    client _write
  done

  for line in $read_combinations; do
    data_version="${line##*=}"
    client_version="${line%%=*}"
    client _read
  done

  # Add checkpoint compatibility tests (only for clusters that support checkpoint endpoints)
  # Skip checkpoint tests for very old clusters that don't have the endpoints
  if [[ "${cluster_version}" < "2.0.0" ]]; then
    echo "Skipping checkpoint compatibility tests for cluster ${cluster_version} (checkpoint endpoints not available)"
  else
    echo ""
    echo "=========================================="
    echo "Running checkpoint compatibility tests"
    echo "=========================================="
    
    # Test 2.0.0 client (if available)
    client_version="2.0.0"
    if [[ " $old_versions " =~ " $client_version " ]]; then
      echo "Testing ${client_version} client against ${cluster_version} cluster"
      client _test_checkpoint_compatibility
    fi
    
    # Test current client
    client_version="${current_version}"
    echo "Testing ${current_version} client against ${cluster_version} cluster"
    client _test_checkpoint_compatibility
  fi

  KEEP_RUNNING=false stop_docker_env
}
