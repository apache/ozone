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

start_docker_env

execute_robot_test scm lib
execute_robot_test scm ozone-lib

execute_robot_test scm basic

execute_robot_test scm gdpr

execute_robot_test scm security/ozone-secure-token.robot

for bucket in link generated; do
  execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket} s3
done

execute_robot_test scm recon

execute_robot_test scm om-ratis

execute_robot_test scm freon

execute_robot_test scm cli
execute_robot_test scm admincli

execute_robot_test scm httpfs

execute_robot_test scm -v SCHEME:ofs -v BUCKET_TYPE:bucket -N ozonefs-simple-ofs-bucket ozonefs/ozonefs.robot
execute_robot_test scm -v SCHEME:o3fs -v BUCKET_TYPE:link -N ozonefs-simple-o3fs-link ozonefs/ozonefs.robot

# running FS tests with different config requires restart of the cluster
export OZONE_KEEP_RESULTS=true
stop_docker_env

## Restarting the cluster with prefix-layout enabled (FSO)
export OZONE_OM_METADATA_LAYOUT=PREFIX
export OZONE_OM_ENABLE_FILESYSTEM_PATHS=true
start_docker_env

execute_robot_test scm -v SCHEME:ofs -v BUCKET_TYPE:link -N ozonefs-prefix-ofs-link ozonefs/ozonefs.robot
execute_robot_test scm -v SCHEME:o3fs -v BUCKET_TYPE:bucket -N ozonefs-prefix-o3fs-bucket ozonefs/ozonefs.robot

execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket}-prefix-layout-objectputget s3/objectputget.robot
execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket}-prefix-layout-objectdelete s3/objectdelete.robot
execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket}-prefix-layout-objectcopy s3/objectcopy.robot
execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket}-prefix-layout-objectmultidelete s3/objectmultidelete.robot
execute_robot_test scm -v BUCKET:${bucket} -N s3-${bucket}-prefix-layout-MultipartUpload s3/MultipartUpload.robot

stop_docker_env

generate_report
