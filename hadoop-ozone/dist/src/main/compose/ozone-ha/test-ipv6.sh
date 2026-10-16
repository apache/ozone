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

#suite:HA-unsecure

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export SCM=scm1
export OM_SERVICE_ID=omservice

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

export COMPOSE_FILE=docker-compose.yaml:ipv6.yaml

# JaCoCo reports to the IPv4 docker bridge gateway; that address is unreachable on
# the IPv6-only compose network and prevents SCM from starting.
if [[ "${OZONE_WITH_COVERAGE:-false}" == "true" ]]; then
  export OZONE_OPTS=
fi

start_docker_env 3

execute_robot_test ${SCM} ipv6

execute_robot_test ${SCM} basic/ozone-shell-single.robot
execute_robot_test ${SCM} -v SCHEME:ofs -v BUCKET_TYPE:bucket -N ozonefs-ofs-bucket ozonefs/ozonefs.robot
execute_robot_test ${SCM} -v BUCKET:generated -v BUCKET_LAYOUT:OBJECT_STORE -N s3-OBJECT_STORE-generated s3/objectputget.robot

execute_robot_test ${SCM} -v USERNAME:httpfs httpfs

execute_robot_test ${SCM} admincli/datanode.robot
execute_robot_test ${SCM} admincli/pipeline.robot
execute_robot_test ${SCM} admincli/scmrole.robot
execute_robot_test ${SCM} omha/om-roles.robot
execute_robot_test ${SCM} omha/om-leader-transfer.robot
