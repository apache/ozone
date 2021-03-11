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

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}
fix_data_dir_permissions

# prepare pre-upgrade cluster
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/generate.robot
execute_robot_test scm -v PREFIX:pre freon/validate.robot
KEEP_RUNNING=false stop_docker_env

# re-start cluster with new version and check after upgrade
export OZONE_KEEP_RESULTS=true
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/validate.robot
# test write key to old bucket after upgrade
execute_robot_test scm -v PREFIX:post freon/generate.robot
execute_robot_test scm -v PREFIX:post freon/validate.robot
stop_docker_env

generate_report
