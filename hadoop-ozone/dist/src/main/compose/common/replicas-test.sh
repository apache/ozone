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

container_db_path="/data/hdds/hdds/"
local_db_backup_path="${COMPOSE_DIR}/container_db_backup_${prefix}"
backup_manifest="${local_db_backup_path}/container_db_paths.tsv"
mkdir -p "${local_db_backup_path}"

echo "Taking backups of existing container.db directories"
datanodes=$(docker ps --format '{{.Names}}' | grep '^ozonesecure-ha-datanode[0-9]\+-1$' | sort)
if [ -z "${datanodes}" ]; then
  echo "Failed to find datanode containers" >&2
  exit 1
fi

>"${backup_manifest}"
for dn_container in ${datanodes}; do
  while read -r db; do
    printf '%s\t%s\n' "${dn_container}" "${db}" >> "${backup_manifest}"
  done < <(docker exec "${dn_container}" find "${container_db_path}" -name "container.db")
done

echo "Stopping datanodes for a consistent container.db backup"
for dn_container in ${datanodes}; do
  if [ "$(docker inspect -f '{{.State.Running}}' "${dn_container}" 2>/dev/null)" = "true" ]; then
    docker stop "${dn_container}" >/dev/null
  else
    echo "${dn_container} is already stopped before backup"
  fi
done

while IFS=$'\t' read -r dn_container db; do
  backup_path="${local_db_backup_path}/${dn_container}${db}"
  mkdir -p "$(dirname "${backup_path}")"
  docker cp "${dn_container}:${db}" "${backup_path}"
done < "${backup_manifest}"

echo "Restarting datanodes after backup"
for dn_container in ${datanodes}; do
  if [ "$(docker inspect -f '{{.State.Running}}' "${dn_container}" 2>/dev/null)" != "true" ]; then
    docker start "${dn_container}" >/dev/null
  fi
done

for dn_container in ${datanodes}; do
  wait_for_datanode "${dn_container}" HEALTHY 60
done

execute_robot_test ${SCM} -v "PREFIX:${prefix}" debug/ozone-debug-tests.robot

# get block locations for key
chunkinfo="${key}-blocks-${prefix}"
docker-compose exec -T ${SCM} bash -c "ozone debug replicas chunk-info ${volume}/${bucket}/${key}" > "$chunkinfo"
host="$(jq -r '.keyLocations[0][0].datanode["hostname"]' ${chunkinfo})"
container="${host%%.*}"
dn_with_num="$(sed -E 's/^.*-(datanode[0-9]+)-[0-9]+$/\1/' <<< "$container")"

datafile="$(jq -r '.keyLocations[0][0].file' ${chunkinfo})"
container_id="$(jq -r '.keyLocations[0][0].blockData.blockID.containerID' ${chunkinfo})"
pipeline_id="$(docker-compose exec -T ${SCM} bash -c \
  "ozone admin container info ${container_id} --json | jq -r '.writePipelineID.id // .writePipelineId.id'")"
if [ -z "${pipeline_id}" ] || [ "${pipeline_id}" = "null" ]; then
  echo "Failed to determine write pipeline for container ${container_id}" >&2
  exit 1
fi

# corrupt the first block of key on one of the datanodes
docker exec "${container}" sed -i -e '1s/^/a/' "${datafile}"

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "CORRUPT_DATANODE:${host}" debug/corrupt-block-checksum.robot

target_container_db=$(docker exec "${container}" bash -c "
  datafile=\$1
  dir=\$(dirname \"\$datafile\")
  while [ \"\$dir\" != '/' ]; do
    if [[ \$(basename \"\$dir\") == CID-* ]]; then
      container_db=\$(find \"\$dir\" -path '*/container.db' | head -n 1)
      if [ -n \"\$container_db\" ]; then
        echo \"\$container_db\"
        exit 0
      fi
      exit 1
    fi
    dir=\$(dirname \"\$dir\")
  done
  exit 1
" _ "${datafile}")
if [ -z "${target_container_db}" ]; then
  echo "Failed to locate container.db for ${datafile} on ${container}" >&2
  exit 1
fi
backup_container_db="${local_db_backup_path}/${container}${target_container_db}"
if [ ! -e "${backup_container_db}" ]; then
  echo "Failed to locate backup for ${target_container_db} on ${container}" >&2
  exit 1
fi

docker stop "${container}"

wait_for_datanode "${container}" STALE 60

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "STALE_DATANODE:${host}" debug/stale-datanode-checksum.robot

docker start "${container}"

wait_for_datanode "${container}" HEALTHY 60

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "DATANODE:${host}" -v "FAULT_INJ_DATANODE:${dn_with_num}" debug/container-state-verifier.robot

execute_robot_test ${OM} kinit.robot
execute_robot_test ${OM} -v "PREFIX:${prefix}" debug/ozone-debug-tests-ec3-2.robot

echo "Overwriting container.db with the backup db"
echo "Restoring backup at ${target_container_db} on ${container}"
echo "Removing dn.ratis state for pipeline ${pipeline_id} on ${container}"
docker stop "${container}"

wait_for_datanode "${container}" STALE 60

container_image="$(docker inspect -f '{{.Config.Image}}' "${container}")"
docker run --rm --volumes-from "${container}" \
  -v "${local_db_backup_path}:${local_db_backup_path}:ro" \
  --entrypoint bash "${container_image}" -c '
    set -euo pipefail
    target_container_db="$1"
    pipeline_id="$2"
    backup_container_db="$3"

    rm -rf "${target_container_db}" "/data/metadata/dn.ratis/${pipeline_id}"
    mkdir -p "$(dirname "${target_container_db}")"
    cp -a "${backup_container_db}" "${target_container_db}"
    chown -R hadoop:hadoop "${target_container_db}"
  ' _ "${target_container_db}" "${pipeline_id}" "${backup_container_db}" || exit 1

docker start "${container}"

wait_for_datanode "${container}" HEALTHY 60

execute_robot_test ${SCM} -v "PREFIX:${prefix}" -v "DATANODE:${host}" debug/block-existence-check.robot
