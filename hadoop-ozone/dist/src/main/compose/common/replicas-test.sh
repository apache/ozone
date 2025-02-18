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

prefix=${RANDOM}

volume="cli-debug-volume${prefix}"
bucket="cli-debug-bucket"
key="testfile"

execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests.robot

# get block locations for key
chunkinfo="${key}-blocks-${prefix}"
docker-compose exec -T ${SCM} bash -c "ozone debug replicas chunk-info ${volume}/${bucket}/${key}" > "$chunkinfo"
host="$(jq -r '.KeyLocations[0][0]["Datanode-HostName"]' ${chunkinfo})"
container="${host%%.*}"

# corrupt the first block of key on one of the datanodes
datafile="$(jq -r '.KeyLocations[0][0].Locations.files[0]' ${chunkinfo})"
docker exec "${container}" sed -i -e '1s/^/a/' "${datafile}"

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "CORRUPT_DATANODE:${host}" debug/ozone-debug-corrupt-block.robot

docker stop "${container}"

wait_for_datanode "${container}" STALE 60
execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "STALE_DATANODE:${host}" debug/ozone-debug-stale-datanode.robot

wait_for_datanode "${container}" DEAD 60
execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-dead-datanode.robot

docker start "${container}"

wait_for_datanode "${container}" HEALTHY 60

start_docker_env 9
execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests-ec3-2.robot
execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests-ec6-3.robot