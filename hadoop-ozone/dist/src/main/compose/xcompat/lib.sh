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

# version is used in bucket name, which does not allow uppercase
current_version="$(echo "${ozone.version}" | sed -e 's/-SNAPSHOT//' | tr '[:upper:]' '[:lower:]')"
# TODO: debug acceptance test failures for client versions 1.0.0 on secure clusters
old_versions="1.1.0 1.2.1 1.3.0 1.4.0 1.4.1" # container is needed for each version in clients.yaml

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

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

  KEEP_RUNNING=false stop_docker_env
}

create_results_dir
