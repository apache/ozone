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

#suite:EC

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

# Replication is EC in client side configs
export COMPOSE_FILE=docker-compose.yaml:ec.yaml
export OZONE_CLIENT_REPLICATION_TYPE=EC

docker-compose up -d --no-recreate --scale datanode=5
wait_for_safemode_exit
execute_robot_test scm -v PREFIX:${RANDOM} -v OZONE_CLIENT_REPLICATION_TYPE:"${OZONE_CLIENT_REPLICATION_TYPE}" ec/basic.robot
docker-compose --ansi never down
