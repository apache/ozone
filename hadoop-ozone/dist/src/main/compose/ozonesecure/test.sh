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

#suite:secure

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export SECURITY_ENABLED=true

: ${OZONE_BUCKET_KEY_NAME:=key1}

start_docker_env

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

execute_robot_test scm kinit.robot

execute_robot_test scm basic

execute_robot_test scm security

for scheme in ofs o3fs; do
  for bucket in link bucket; do
    execute_robot_test scm -v SCHEME:${scheme} -v BUCKET_TYPE:${bucket} -N ozonefs-${scheme}-${bucket} ozonefs/ozonefs.robot
  done
done

for bucket in link generated; do
  execute_robot_test s3g -v BUCKET:${bucket} -N s3-${bucket} s3
done

#expects 4 pipelines, should be run before 
#admincli which creates STANDALONE pipeline
execute_robot_test scm recon

# test replication; must be run before admincli,
# which stops replication manager
docker-compose up -d --scale datanode=2
execute_robot_test scm -v container:1 -v count:2 replication/wait.robot
docker-compose up -d --scale datanode=3
execute_robot_test scm -v container:1 -v count:3 replication/wait.robot

execute_robot_test scm admincli
execute_robot_test scm spnego

stop_docker_env

generate_report
