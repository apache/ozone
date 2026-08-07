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

#suite:unsecure

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SCM=local
export SECURITY_ENABLED=false

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

# The single ozone local container has no separate SCM/OM services; the
# published S3 Gateway and Recon ports signal that the runtime is ready.
wait_for_safemode_exit() {
  wait_for_port 127.0.0.1 9878 180
  wait_for_port 127.0.0.1 9888 180
}

wait_for_om_leader() {
  return 0
}

start_docker_env 1

execute_robot_test local ozonelocal/locals3.robot
