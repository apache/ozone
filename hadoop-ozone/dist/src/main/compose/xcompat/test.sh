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

# version is used in bucket name, which does not allow uppercase
current_version="$(echo "${ozone.version}" | sed -e 's/-SNAPSHOT//' | tr '[:upper:]' '[:lower:]')"
# TODO: debug acceptance test failures for client versions 1.0.0 on secure clusters
old_versions="1.1.0 1.2.1 1.3.0 1.4.0" # container is needed for each version in clients.yaml

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

export SECURITY_ENABLED=true
: ${OZONE_BUCKET_KEY_NAME:=key1}

dd if=/dev/urandom of=${TEST_DATA_DIR}/1mb bs=1048576 count=1 >/dev/null 2>&1
dd if=/dev/urandom of=${TEST_DATA_DIR}/2mb bs=1048576 count=2 >/dev/null 2>&1
dd if=/dev/urandom of=${TEST_DATA_DIR}/3mb bs=1048576 count=3 >/dev/null 2>&1

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
    -v SUFFIX:${client_version} \
    compatibility/write.robot
}

_read() {
  _kinit
  local data_version="$1"
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-read-${data_version}" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v DATA_VERSION:${data_version} \
    -v SUFFIX:${data_version} \
    compatibility/read.robot
}

_read_ec() {
  _kinit
  local data_version="$1"
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-EC-read" \
    -v CLIENT_VERSION:${client_version} \
    -v CLUSTER_VERSION:${cluster_version} \
    -v DATA_VERSION:${data_version} \
    -v SUFFIX:${data_version} \
    --include test-ec-compat ec/backward-compat.robot
}

test_cross_compatibility() {
  echo "Starting ${cluster_version} cluster with COMPOSE_FILE=${COMPOSE_FILE}"

  OZONE_KEEP_RESULTS=true start_docker_env 5

  execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

  _init

  new_client _write
  new_client _read ${current_version}

  for client_version in "$@"; do
    client="old_client_${client_version//./_}"

    old_client _write
    old_client _read ${client_version}

    old_client _read ${current_version}
    new_client _read ${client_version}
  done

  if [[ "${cluster_version}" == "${current_version}" ]]; then # until HDDS-11334
    echo "Running Erasure Coded storage backward compatibility tests."

    local non_ec_client_versions="1.1.0 1.2.1"
    for client_version in ${non_ec_client_versions}; do
      client="old_client_${client_version//./_}"
      unset OUTPUT_PATH # FIXME why is it unset?
      old_client _read_ec ${current_version}
    done
  fi

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
