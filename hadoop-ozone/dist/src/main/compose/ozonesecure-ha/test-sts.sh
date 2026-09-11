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

#suite:sts

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

# shellcheck source=/dev/null
source "$COMPOSE_DIR/ranger-testlib.sh"

setup_ranger_acceptance_env

execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-sts.robot
execute_robot_test s3g -v RANGER_ENDPOINT_URL:"http://ranger:6080" -v USER:hdfs security/ozone-secure-sts-multitenant.robot
"${COMPOSE_DIR}/polaris-smoketest.sh"
