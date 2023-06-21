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

execute_robot_test s3g kinit.robot

# verify root CA rotation monitor task is active on leader
wait_for_execute_command scm 30 "jps | grep StorageContainerManagerStarter | awk -F' ' '{print $1}' | xargs -I {} jstack {} | grep 'RootCARotationManager-Active'"

# wait and verify root CA is rotated
wait_for_execute_command scm 90 "ozone admin cert info 2"

# verify scm operations
execute_robot_test s3g admincli/pipeline.robot

# wait for second root CA rotation
wait_for_execute_command scm 90 "ozone admin cert info 3"

# verify om operations
wait_for_execute_command scm 10 "ozone sh volume create rotation-vol"
wait_for_execute_command scm 10 "ozone sh bucket create rotation-vol/rotation-bucket"

# verify data read write
wait_for_execute_command scm 10 "ozone sh key put /opt/hadoop/README.md  /rotation-vol/rotation-bucket/README.md"
wait_for_execute_command scm 10 "ozone sh key get /opt/hadoop/README.md.1  /rotation-vol/rotation-bucket/README.md"

# check the metrics
execute_robot_test scm scmha/root-ca-rotation.robot

stop_docker_env

generate_report
