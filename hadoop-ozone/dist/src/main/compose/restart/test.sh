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

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

create_data_dirs dn{1..3} om recon s3g scm

# prepare pre-upgrade cluster
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/generate.robot
execute_robot_test scm -v PREFIX:pre freon/validate.robot
execute_robot_test scm -v PREFIX:pre freon/generate-chunk.robot
execute_robot_test scm -v PREFIX:pre freon/validate-chunk.robot
KEEP_RUNNING=false stop_docker_env

# re-start cluster with new version and check after upgrade
export OZONE_KEEP_RESULTS=true
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/validate.robot
execute_robot_test scm -v PREFIX:pre freon/validate-chunk.robot
# test write key to old bucket after upgrade
execute_robot_test scm -v PREFIX:post freon/generate.robot
execute_robot_test scm -v PREFIX:post freon/validate.robot
execute_robot_test scm -v PREFIX:post freon/generate-chunk.robot
execute_robot_test scm -v PREFIX:post freon/validate-chunk.robot
