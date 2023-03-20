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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=false
export OZONE_REPLICATION_FACTOR=3
export SCM=scm1
export OM_SERVICE_ID=omservice

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env 5

execute_robot_test ${SCM} basic/ozone-shell-single.robot
execute_robot_test ${SCM} basic/links.robot

execute_robot_test ${SCM} -v SCHEME:ofs -v BUCKET_TYPE:link -N ozonefs-ofs-link ozonefs/ozonefs.robot

exclude=""
for bucket in generated; do
  execute_robot_test ${SCM} -v BUCKET:${bucket} -N s3-${bucket} ${exclude} s3
  # some tests are independent of the bucket type, only need to be run once
  exclude="--exclude no-bucket-type"
done

execute_robot_test ${SCM} freon
execute_robot_test ${SCM} -v USERNAME:httpfs httpfs

stop_docker_env

generate_report
