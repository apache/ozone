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

dn_container="ozonesecure-ha-datanode1-1"
container_db_path="/data/hdds/hdds/"
local_db_backup_path="${COMPOSE_DIR}/container_db_backup"
mkdir -p "${local_db_backup_path}"

echo "Taking a backup of container.db"
docker exec "${dn_container}" find "${container_db_path}" -name "container.db" | while read -r db; do
  docker cp "${dn_container}:${db}" "${local_db_backup_path}/container.db"
done

execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests.robot

# get block locations for key
chunkinfo="${key}-blocks-${prefix}"
docker-compose exec -T ${SCM} bash -c "ozone debug replicas chunk-info ${volume}/${bucket}/${key}" > "$chunkinfo"
host="$(jq -r '.keyLocations[0][0].datanode["hostname"]' ${chunkinfo})"
container="${host%%.*}"
dn_with_num="$(sed -E 's/^.*-(datanode[0-9]+)-[0-9]+$/\1/' <<< "$container")"

# corrupt the first block of key on one of the datanodes
datafile="$(jq -r '.keyLocations[0][0].file' ${chunkinfo})"
docker exec "${container}" sed -i -e '1s/^/a/' "${datafile}"

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "CORRUPT_DATANODE:${host}" debug/corrupt-block-checksum.robot

echo "Overwriting container.db with the backup db"
target_container_dir=$(docker exec "${container}" find "${container_db_path}" -name "container.db" | xargs dirname)
docker cp "${local_db_backup_path}/container.db" "${container}:${target_container_dir}/"
docker exec "${container}" sudo chown -R hadoop:hadoop "${target_container_dir}"

docker stop "${container}"

wait_for_datanode "${container}" STALE 60
execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "STALE_DATANODE:${host}" debug/stale-datanode-checksum.robot

docker start "${container}"

wait_for_datanode "${container}" HEALTHY 60

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "DATANODE:${host}" debug/block-existence-check.robot

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "DATANODE:${host}" -v "FAULT_INJ_DATANODE:${dn_with_num}" debug/container-state-verifier.robot

execute_robot_test ${OM} kinit.robot
execute_robot_test ${OM} -v "PREFIX:${prefix}" debug/ozone-debug-tests-ec3-2.robot
