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

current_version=1.2.0
previous_version=1.1.0

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

_createbucket() {
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-createbucket" -v SUFFIX:${cluster_version} compatibility/createbucket.robot
}

_createdirs() {
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-createbucket" -v SUFFIX:${cluster_version} compatibility/createdirs.robot
}

_rename() {
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-rename" -v SUFFIX:${cluster_version} compatibility/rename.robot
}

_delete() {
  execute_robot_test ${container} -N "xcompat-cluster-${cluster_version}-client-${client_version}-delete" -v SUFFIX:${cluster_version} compatibility/delete.robot
}

test_cross_compat_buck_layout() {
  echo "Starting cluster with COMPOSE_FILE=${COMPOSE_FILE}"

  OZONE_KEEP_RESULTS=true start_docker_env

  # execute_command_in_container scm ozone freon ockg -n1 -t1 -p warmup
  new_client _createbucket
  new_client _createdirs

  client=$(docker ps | grep _old_client_ | awk '{ print $NF }')
  echo "Client=${client}"

  client=${client#${basename}_}
  client=${client%_1}
  client_version=${client#old_client_}
  client_version=${client_version//_/.}

  echo "Client-Version=${client_version}"
  old_client _rename
  old_client _delete

  stop_docker_env
}

create_results_dir

# current cluster with various clients
COMPOSE_FILE=new-cluster.yaml:clients.yaml cluster_version=${current_version} test_cross_compat_buck_layout

generate_report
