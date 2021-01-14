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

#suite:misc

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=1

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

wait_for_safemode_exit() {
  wait_for_port scm 22 30
  wait_for_port om 22 30
  wait_for_port datanode 22 30
}

start_docker_env 1

${COMPOSE_DIR}/start.sh
${COMPOSE_DIR}/ps.sh

execute_robot_test scm admincli/pipeline.robot

${COMPOSE_DIR}/stop.sh

stop_docker_env

generate_report
