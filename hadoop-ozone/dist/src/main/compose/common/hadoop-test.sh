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

: ${HADOOP_IMAGE:="${docker.hadoop.image}"}
: ${HADOOP_TEST_IMAGES:=""}

if [[ -z "${HADOOP_TEST_IMAGES}" ]]; then
  # hadoop2 image is only available from Docker Hub
  HADOOP_TEST_IMAGES="${HADOOP_TEST_IMAGES} apache/hadoop:${hadoop2.version}"
  HADOOP_TEST_IMAGES="${HADOOP_TEST_IMAGES} ${HADOOP_IMAGE}:3.3.6"
  HADOOP_TEST_IMAGES="${HADOOP_TEST_IMAGES} ${HADOOP_IMAGE}:${hadoop.version}${docker.hadoop.image.flavor}"
fi

export HADOOP_MAJOR_VERSION=3
export HADOOP_TEST_IMAGE="${HADOOP_IMAGE}:${hadoop.version}"
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

for HADOOP_TEST_IMAGE in $HADOOP_TEST_IMAGES; do
  export HADOOP_TEST_IMAGE
  hadoop_version=$(docker run --rm "${HADOOP_TEST_IMAGE}" bash -c "hadoop version | grep -m1 '^Hadoop' | cut -f2 -d' '")
  export HADOOP_MAJOR_VERSION=${hadoop_version%%.*}

  docker-compose --ansi never --profile hadoop up -d nm rm

  execute_command_in_container rm hadoop version

  if [[ ${SECURITY_ENABLED} == "true" ]]; then
    execute_robot_test rm -v SECURITY_ENABLED:"${SECURITY_ENABLED}" kinit-hadoop.robot
  fi

  for scheme in o3fs ofs; do
    execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${hadoop_version}-hadoopfs-${scheme}" ozonefs/hadoopo3fs.robot
    # TODO secure MapReduce test is failing with 2.7 due to some token problem
    if [[ ${SECURITY_ENABLED} != "true" ]] || [[ ${HADOOP_MAJOR_VERSION} == "3" ]]; then
      execute_robot_test rm -v "SCHEME:${scheme}" -N "hadoop-${hadoop_version}-mapreduce-${scheme}" mapreduce.robot
    fi
  done

  save_container_logs nm rm
  stop_containers nm rm
done
