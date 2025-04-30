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

#suite:leadership

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:scm-decommission.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

# bootstrap new SCM4
docker-compose up -d scm4.org
wait_for_port scm4.org 9894 120
execute_robot_test scm4.org kinit.robot
wait_for_execute_command scm4.org 120 "ozone admin scm roles | grep scm4.org"
execute_robot_test scm4.org scmha/primordial-scm.robot

# add new datanode4
docker-compose up -d datanode4
wait_for_port datanode4 9856 120
wait_for_execute_command scm4.org 60 "ozone admin datanode list | grep datanode4"

# decommission primordial node scm1.org
SCMID=$(execute_command_in_container scm4.org bash -c "ozone admin scm roles" | grep scm4 | awk -F: '{print $4}')
docker-compose stop scm4.org
execute_robot_test scm3.org kinit.robot
wait_for_execute_command scm3.org 60 "ozone admin scm decommission --nodeid=${SCMID} | grep Decommissioned"
execute_robot_test s3g scmha/scm-decommission.robot
