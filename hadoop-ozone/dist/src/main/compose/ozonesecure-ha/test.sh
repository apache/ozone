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

#suite:HA

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="id1"
export SCM=scm1.org

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_robot_test ${SCM} kinit.robot

execute_robot_test ${SCM} freon

execute_robot_test ${SCM} basic/links.robot

execute_robot_test ${SCM} s3

execute_robot_test ${SCM} admincli

export SCM=scm2.org
execute_robot_test ${SCM} admincli
stop_docker_env

generate_report
