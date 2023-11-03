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

#suite:cert-rotation

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:scm-decommission.yaml:root-ca-rotation.yaml

: ${OZONE_BUCKET_KEY_NAME:=key1}

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

execute_robot_test scm1.org kinit.robot

# verify root CA rotation monitor task is active on leader
wait_for_execute_command scm1.org 30 "jps | grep StorageContainerManagerStarter | sed 's/StorageContainerManagerStarter//' | xargs  | xargs -I {} jstack {} | grep 'RootCARotationManager-Active'"

# wait and verify root CA is rotated
wait_for_root_certificate scm1.org 240 2

# transfer leader to scm2.org
execute_robot_test scm1.org scmha/scm-leader-transfer.robot
wait_for_execute_command scm1.org 30 "jps | grep StorageContainerManagerStarter | sed 's/StorageContainerManagerStarter//' | xargs | xargs -I {} jstack {} | grep 'RootCARotationManager-Inactive'"
execute_robot_test scm1.org -v PREFIX:"rootca" certrotation/root-ca-rotation-client-checks.robot

# verify om operations
execute_commands_in_container scm1.org "ozone sh volume create /r-v1 && ozone sh bucket create /r-v1/r-b1"

# verify scm operations
execute_robot_test scm1.org admincli/pipeline.robot

# wait for next root CA rotation
wait_for_root_certificate scm1.org 240 3

# bootstrap new SCM4 and verify certificate
docker-compose up -d scm4.org
wait_for_port scm4.org 9894 120
execute_robot_test scm4.org kinit.robot
wait_for_execute_command scm4.org 120 "ozone admin scm roles | grep scm4.org"
wait_for_execute_command scm4.org 30 "ozone admin cert list --role=scm -c 100| grep scm4.org"

# wait for next root CA rotation
wait_for_root_certificate scm4.org 240 4

execute_robot_test om1 kinit.robot
execute_robot_test om2 kinit.robot
execute_robot_test om3 kinit.robot
check_root_ca_file_cmd="ozone admin cert list --role=scm -c 100 | grep -v 'scm-sub' | grep 'scm'  | cut -d ' ' -f 1 | sort | tail -n 1 | xargs -I {} echo /data/metadata/om/certs/ROOTCA-{}.crt | xargs find"
wait_for_execute_command om1 30 $check_root_ca_file_cmd
wait_for_execute_command om2 30 $check_root_ca_file_cmd
wait_for_execute_command om3 30 $check_root_ca_file_cmd
execute_robot_test scm4.org -v PREFIX:"rootca2" certrotation/root-ca-rotation-client-checks.robot

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
wait_for_root_certificate scm3.org 240 5

#decomission scm3.org
execute_robot_test scm1.org scmha/scm-decommission.robot

# check the metrics
execute_robot_test scm2.org scmha/root-ca-rotation.robot

stop_docker_env

generate_report
