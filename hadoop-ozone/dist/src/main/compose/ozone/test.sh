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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env 5

execute_robot_test scm lib
execute_robot_test scm ozone-lib

execute_robot_test om auditparser

execute_robot_test scm basic

execute_robot_test scm gdpr

execute_robot_test scm security/ozone-secure-token.robot

for bucket in erasure link generated; do
  execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket} s3
done

execute_robot_test scm recon

execute_robot_test scm om-ratis

execute_robot_test scm freon

execute_robot_test scm cli
execute_robot_test scm admincli

execute_debug_tests

execute_robot_test scm -v SCHEME:ofs -v BUCKET_TYPE:link -N ozonefs-fso-ofs-link ozonefs/ozonefs.robot
execute_robot_test scm -v SCHEME:o3fs -v BUCKET_TYPE:bucket -N ozonefs-fso-o3fs-bucket ozonefs/ozonefs.robot

execute_robot_test scm ec

stop_docker_env

generate_report
