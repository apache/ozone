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

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

if [[ ${SECURITY_ENABLED} == "true" ]]; then
  execute_robot_test s3g kinit.robot
  # TODO get secret
else
  AWS_ACCESS_KEY_ID=s3a-contract
  AWS_SECRET_ACCESS_KEY=unsecure
fi

OZONE_S3G_ADDRESS=http://localhost:9878

execute_command_in_container s3g ozone sh bucket create --layout OBJECT_STORE "/s3v/obs-bucket"
execute_command_in_container s3g ozone sh bucket create --layout LEGACY "/s3v/leg-bucket"
execute_command_in_container s3g ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED "/s3v/fso-bucket"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY OZONE_S3G_ADDRESS

for bucket in obs-bucket leg-bucket fso-bucket; do
  execute_s3a_tests "$bucket"
done
