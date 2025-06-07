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

start_docker_env 5

## Exclude virtual-host tests. This is tested separately as it requires additional config.
execute_robot_test scm -v BUCKET:erasure --exclude virtual-host s3

execute_robot_test scm ec/rewrite.robot

prefix=${RANDOM}
execute_robot_test scm -v PREFIX:${prefix} ec/basic.robot
docker-compose up -d --no-recreate --scale datanode=4
execute_robot_test scm -v PREFIX:${prefix} -N read-4-datanodes ec/read.robot
docker-compose up -d --no-recreate --scale datanode=3
execute_robot_test scm -v PREFIX:${prefix} -N read-3-datanodes ec/read.robot
docker-compose up -d --no-recreate --scale datanode=5
execute_robot_test scm -v container:1 -v count:5 -N EC-recovery replication/wait.robot
docker-compose up -d --no-recreate --scale datanode=9
execute_robot_test scm -N S3-EC-Storage ec/awss3ecstorage.robot
