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

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

current_version="${OZONE_CURRENT_VERSION}"
# TODO: debug acceptance test failures for client versions 1.0.0 on secure clusters
old_versions="1.1.0 1.2.1 1.3.0 1.4.1 2.0.0" # container is needed for each version in clients.yaml

export SECURITY_ENABLED=true
: ${OZONE_BUCKET_KEY_NAME:=key1}

echo 'Compatibility Test' > "${TEST_DATA_DIR}"/small

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
  local data_version="$1"
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-read-${data_version}" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v DATA_VERSION:${data_version} \
    -v TEST_DATA_DIR:/testdata \
    compatibility/read.robot
}

test_cross_compatibility() {
  echo "Starting ${cluster_version} cluster with COMPOSE_FILE=${COMPOSE_FILE}"

  OZONE_KEEP_RESULTS=true start_docker_env 5

  execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

  _init

  # first write with client matching cluster version
  client_version="${cluster_version}" client _write

  for client_version in "$@"; do
    # skip write, since already done
    if [[ "${client_version}" == "${cluster_version}" ]]; then
      continue
    fi
    client _write
  done

  for client_version in "$@"; do
    for data_version in $(echo "$client_version" "$cluster_version" "$current_version" | xargs -n1 | sort -u); do

      # do not test old-only scenario
      if [[ "${cluster_version}" != "${current_version}" ]] \
        && [[ "${client_version}" != "${current_version}" ]] \
        && [[ "${data_version}" != "${current_version}" ]]; then
        continue
      fi

      client _read ${data_version}
    done
  done

    # NEW: Add checkpoint compatibility tests
  # Give OM a moment to be fully ready for HTTP requests
  echo "Waiting for OM to be ready for HTTP requests..."
  sleep 10

  echo ""
  echo "=========================================="
  echo "Running checkpoint compatibility tests"
  echo "=========================================="
  for client_version in ${checkpoint_client_versions}; do
    client _test_checkpoint_compatibility
  done

  KEEP_RUNNING=false stop_docker_env
}

_get_om_hostname() {
  # Get OM hostname from the cluster configuration
  echo "om"  # Default OM service name in docker-compose
}

_download_checkpoint_v1() {
  _kinit
  local om_host=$(_get_om_hostname)
  local expected_result="$1"

  echo "Testing /dbCheckpoint endpoint: client ${client_version} → cluster ${cluster_version}"

  # Add debugging information
  echo "DEBUG: Using container: ${container}"
  echo "DEBUG: Using OM host: ${om_host}"
  
  # Check if OM is reachable
  echo "DEBUG: Testing OM connectivity..."
  execute_command_in_container ${container} curl -v -s --connect-timeout 5 "http://${om_host}:9874/" || echo "DEBUG: Basic OM connectivity failed"
  
  # List running OM processes for debugging
  echo "DEBUG: Checking OM processes..."
  execute_command_in_container om ps aux | grep -i ozone || echo "DEBUG: No OM processes found"
  
  # Check if the specific endpoint exists
  echo "DEBUG: Testing if dbCheckpoint endpoint exists..."
  execute_command_in_container ${container} curl -v -s --connect-timeout 5 "http://${om_host}:9874/dbCheckpoint" || echo "DEBUG: dbCheckpoint endpoint test failed"

  # Download using original checkpoint endpoint
  local download_cmd="curl -f -s -o /tmp/checkpoint_v1_${client_version}.tar.gz http://${om_host}:9874/dbCheckpoint"
  echo "DEBUG: Executing: ${download_cmd}"

  if execute_command_in_container ${container} bash -c "${download_cmd}"; then
    local actual_result="pass"
    echo "✓ Successfully downloaded checkpoint via v1 endpoint"
    # Show file info for verification
    execute_command_in_container ${container} ls -la /tmp/checkpoint_v1_${client_version}.tar.gz || true
  else
    local actual_result="fail"
    echo "✗ Failed to download checkpoint via v1 endpoint"
  fi

  if [[ "${expected_result}" == "${actual_result}" ]]; then
    echo "✓ EXPECTED: ${expected_result}, GOT: ${actual_result}"
    return 0
  else
    echo "✗ EXPECTED: ${expected_result}, GOT: ${actual_result}"
    return 1
  fi
}

_download_checkpoint_v2() {
  _kinit
  local om_host=$(_get_om_hostname)
  local expected_result="$1"

  echo "Testing /dbCheckpointv2 endpoint: client ${client_version} → cluster ${cluster_version}"

  # Add debugging information (similar to v1 but for v2 endpoint)
  echo "DEBUG: Using container: ${container}"
  echo "DEBUG: Using OM host: ${om_host}"
  
  # Check if the specific v2 endpoint exists
  echo "DEBUG: Testing if dbCheckpointv2 endpoint exists..."
  execute_command_in_container ${container} curl -v -s --connect-timeout 5 "http://${om_host}:9874/dbCheckpointv2" || echo "DEBUG: dbCheckpointv2 endpoint test failed"

  # Download using new checkpointv2 endpoint
  local download_cmd="curl -f -s -o /tmp/checkpoint_v2_${client_version}.tar.gz http://${om_host}:9874/dbCheckpointv2"
  echo "DEBUG: Executing: ${download_cmd}"

  if execute_command_in_container ${container} bash -c "${download_cmd}"; then
    local actual_result="pass"
    echo "✓ Successfully downloaded checkpoint via v2 endpoint"
    # Show file info for verification
    execute_command_in_container ${container} ls -la /tmp/checkpoint_v2_${client_version}.tar.gz || true
  else
    local actual_result="fail"
    echo "✗ Failed to download checkpoint via v2 endpoint"
  fi

  if [[ "${expected_result}" == "${actual_result}" ]]; then
    echo "✓ EXPECTED: ${expected_result}, GOT: ${actual_result}"
    return 0
  else
    echo "✗ EXPECTED: ${expected_result}, GOT: ${actual_result}"
    return 1
  fi
}

_test_checkpoint_compatibility() {
  local test_result=0

  # Determine client and cluster types
  local is_old_client=false
  local is_old_cluster=false

  if [[ "${client_version}" != "${current_version}" ]]; then
    is_old_client=true
  fi

  if [[ "${cluster_version}" != "${current_version}" ]]; then
    is_old_cluster=true
  fi

  echo ""
  echo "=== CHECKPOINT COMPATIBILITY TEST ==="
  echo "Client: ${client_version} ($([ "$is_old_client" = true ] && echo "OLD" || echo "NEW"))"
  echo "Cluster: ${cluster_version} ($([ "$is_old_cluster" = true ] && echo "OLD" || echo "NEW"))"
  echo "====================================="

  # Test v1 endpoint (/dbCheckpoint)
  echo "→ Testing v1 endpoint compatibility..."
  # Both old and new clusters should serve v1 endpoint (backward compatibility)
  client _download_checkpoint_v1 "pass" || test_result=1

  # Test v2 endpoint (/dbCheckpointv2)
  echo "→ Testing v2 endpoint compatibility..."
  if [ "$is_old_cluster" = true ]; then
    # Old cluster doesn't have v2 endpoint
    if [ "$is_old_client" = false ]; then
      # New client hitting v2 on old cluster should fail
      client _download_checkpoint_v2 "fail" || test_result=1
    fi
    # Old client won't try v2 endpoint
  else
    # New cluster has v2 endpoint
    if [ "$is_old_client" = false ]; then
      # New client should successfully use v2 endpoint
      client _download_checkpoint_v2 "pass" || test_result=1
    fi
    # Old client doesn't know about v2 endpoint
  fi

  if [ $test_result -eq 0 ]; then
    echo "✓ All checkpoint compatibility tests PASSED"
  else
    echo "✗ Some checkpoint compatibility tests FAILED"
  fi

  return $test_result
}

create_results_dir
