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

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org

: ${OZONE_BUCKET_KEY_NAME:=key1}

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_command_in_container kms hadoop key create ${OZONE_BUCKET_KEY_NAME}

#Run this test before kinit on a SCM HA secure cluster
execute_robot_test s3g scmha/container-create.robot

execute_robot_test s3g kinit.robot

execute_robot_test s3g freon

execute_robot_test s3g -v SCHEME:o3fs -v BUCKET_TYPE:link -N ozonefs-o3fs-link ozonefs/ozonefs.robot

execute_robot_test s3g basic/links.robot

## Exclude virtual-host tests. This is tested separately as it requires additional config.
exclude="--exclude virtual-host"
for bucket in link; do
  execute_robot_test s3g -v BUCKET:${bucket} -N s3-${bucket} ${exclude} s3
  # some tests are independent of the bucket type, only need to be run once
  ## Exclude virtual-host.robot
  exclude="--exclude virtual-host --exclude no-bucket-type"
done

# Run Fault Injection tests at the end
execute_robot_test s3g ozone-fi/byteman_faults_sample.robot
