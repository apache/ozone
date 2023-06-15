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

startOMs(){
    docker-compose exec -T om1 /opt/startOM.sh
    docker-compose exec -T om2 /opt/startOM.sh
    docker-compose exec -T om3 /opt/startOM.sh
}

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR
export OM_SERVICE_ID="omservice"

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

# Start OMs separately. In this test, the OMs will be stopped and restarted multiple times.
# So we do not want the container to be tied to the OM process.
startOMs

execute_robot_test scm omha/testOMHA.robot

stop_docker_env

generate_report
