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

extra_compose_file=hadoop.yaml
if [[ ${SECURITY_ENABLED} == "true" ]]; then
  extra_compose_file=hadoop-secure.yaml
fi
export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yaml}":../common/${extra_compose_file}

export HADOOP_MAJOR_VERSION=3
export HADOOP_VERSION=unused # will be set for each test version below
export OZONE_REPLICATION_FACTOR=3

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

if [[ ${SECURITY_ENABLED} == "true" ]]; then
  execute_robot_test ${SCM} kinit.robot
fi

execute_robot_test ${SCM} createmrenv.robot

# reinitialize the directories to use
export OZONE_DIR=/opt/ozone

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

for HADOOP_VERSION in ${hadoop2.version} 3.1.2 ${hadoop.version}; do
  export HADOOP_VERSION
  export HADOOP_MAJOR_VERSION=${HADOOP_VERSION%%.*}
  if [[ "${HADOOP_VERSION}" == "${hadoop2.version}" ]] || [[ "${HADOOP_VERSION}" == "${hadoop.version}" ]]; then
    export HADOOP_IMAGE=apache/hadoop
  else
    export HADOOP_IMAGE=flokkr/hadoop
  fi

  docker-compose --ansi never --profile hadoop up -d nm rm

  execute_command_in_container rm hadoop version

  if [[ ${SECURITY_ENABLED} == "true" ]]; then
    execute_robot_test rm kinit-hadoop.robot
  fi

  for scheme in o3fs ofs; do
    execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${HADOOP_VERSION}-hadoopfs-${scheme}" ozonefs/hadoopo3fs.robot
    # TODO secure MapReduce test is failing with 2.7 due to some token problem
    if [[ ${SECURITY_ENABLED} != "true" ]] || [[ ${HADOOP_MAJOR_VERSION} == "3" ]]; then
      execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${HADOOP_VERSION}-mapreduce-${scheme}" mapreduce.robot
    fi
  done

  save_container_logs nm rm
  stop_containers nm rm
done
