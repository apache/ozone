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

export COMPOSE_FILE=docker-compose.yaml:../common/hadoop.yaml
export HADOOP_MAJOR_VERSION=3
export HADOOP_VERSION=unused # will be set for each test version below
export OZONE_REPLICATION_FACTOR=3

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

execute_robot_test ${SCM} createmrenv.robot

# reinitialize the directories to use
export OZONE_DIR=/opt/ozone

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

for HADOOP_VERSION in 2.7.3 3.1.2 3.2.2 3.3.5; do
  export HADOOP_VERSION
  export HADOOP_MAJOR_VERSION=${HADOOP_VERSION%%.*}
  # Check if $HADOOP_VERSION starts with the prefix "3.3."
  if [[ $HADOOP_VERSION == 3.3.* ]]; then
    export HADOOP_IMAGE=apache/hadoop
  else
    export HADOOP_IMAGE=flokkr/hadoop
  fi

  docker-compose --ansi never --profile hadoop up -d nm rm

  execute_command_in_container rm hadoop version

  for scheme in o3fs ofs; do
    execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${HADOOP_VERSION}-hadoopfs-${scheme}" ozonefs/hadoopo3fs.robot
    execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${HADOOP_VERSION}-mapreduce-${scheme}" mapreduce.robot
  done

  save_container_logs nm rm
  stop_containers nm rm
done

stop_docker_env

generate_report
