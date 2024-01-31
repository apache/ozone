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

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env 4

execute_robot_test scm basic/basic.robot

execute_robot_test scm topology/cli.robot

execute_robot_test scm recon

# Ensure data can be read even when a full rack
# is stopped.
execute_robot_test scm topology/loaddata.robot

stop_containers datanode_1 datanode_2 datanode_3

execute_robot_test scm -N readdata-first-half topology/readdata.robot

start_containers datanode_1 datanode_2 datanode_3

wait_for_port datanode_1 9858 60
wait_for_port datanode_2 9858 60
wait_for_port datanode_3 9858 60

stop_containers datanode_4 datanode_5 datanode_6

execute_robot_test scm -N readdata-second-half topology/readdata.robot
