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

#suite:HA-secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:root-ca-rotation.yaml

: ${OZONE_BUCKET_KEY_NAME:=key1}

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

execute_robot_test scm1.org kinit.robot

# verify root CA rotation monitor task is active on leader
wait_for_execute_command scm1.org 30 "jps | grep StorageContainerManagerStarter | sed 's/StorageContainerManagerStarter//' | xargs  | xargs -I {} jstack {} | grep 'RootCARotationManager-Active'"

# wait and verify root CA is rotated
wait_for_execute_command scm1.org 240 "ozone admin cert info 2"

# transfer leader to scm2.org
execute_robot_test scm1.org scmha/scm-leader-transfer.robot
wait_for_execute_command scm1.org 30 "jps | grep StorageContainerManagerStarter | sed 's/StorageContainerManagerStarter//' | xargs | xargs -I {} jstack {} | grep 'RootCARotationManager-Inactive'"

# verify om operations
execute_commands_in_container scm1.org "ozone sh volume create /r-v1 && ozone sh bucket create /r-v1/r-b1"

# verify scm operations
execute_robot_test scm1.org admincli/pipeline.robot

# wait for next root CA rotation
wait_for_execute_command scm1.org 240 "ozone admin cert info 3"

# bootstrap new SCM4 and verify certificate
docker-compose up -d scm4.org
wait_for_port scm4.org 9894 120
execute_robot_test scm4.org kinit.robot
wait_for_execute_command scm4.org 120 "ozone admin scm roles | grep scm4.org"
wait_for_execute_command scm4.org 30 "ozone admin cert list --role=scm | grep scm4.org"

# wait for next root CA rotation
wait_for_execute_command scm4.org 240 "ozone admin cert info 4"

#transfer leader to scm4.org
execute_robot_test scm4.org -v "TARGET_SCM:scm4.org" scmha/scm-leader-transfer.robot

# add new datanode4 and verify certificate
docker-compose up -d datanode4
wait_for_port datanode4 9856 120
wait_for_execute_command scm4.org 60 "ozone admin datanode list | grep datanode4"

#transfer leader to scm3.org
execute_robot_test scm3.org kinit.robot
execute_robot_test scm4.org  -v "TARGET_SCM:scm3.org" scmha/scm-leader-transfer.robot

# wait for next root CA rotation
wait_for_execute_command scm3.org 240 "ozone admin cert info 5"

#decomission scm1.org
execute_robot_test scm3.org scmha/scm-decommission.robot

# check the metrics
execute_robot_test scm2.org scmha/root-ca-rotation.robot

stop_docker_env

generate_report
