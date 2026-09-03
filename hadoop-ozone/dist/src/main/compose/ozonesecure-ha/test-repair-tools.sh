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

#suite:tools

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID=omservice
export SCM=scm1.org
export OM=om1
export COMPOSE_FILE=docker-compose.yaml:debug-tools.yaml
export OZONE_DIR=/opt/hadoop

: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

create_data_dirs dn{1..5} kms om{1..3} recon s3g scm{1..3}

start_docker_env

repair_and_restart_om() {
  local om_container="$1"
  local om_id="$2"
  echo "Waiting for container '${om_container}' to stop..."
  # Loop until the container is not running
  timeout=60  # seconds
  start_time=$(date +%s)
  while [ "$(docker inspect -f '{{.State.Running}}' "${om_container}" 2>/dev/null)" == "true" ]; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))

    if [ "$elapsed" -ge "$timeout" ]; then
      echo "Timeout: Container '${om_container}' did not stop within ${timeout} seconds."
      exit 1
    fi
    sleep 1
  done
  echo "Container '${om_container}' has stopped."

  logpath=$(execute_command_in_container ${SCM} bash -c "find / -type f -path '/*/$om_id/*/log_inprogress_0' 2>/dev/null | head -n 1")
  echo "Ratis log segment file path: ${logpath}"

  execute_command_in_container ${SCM} bash -c "echo y | ozone repair om srt -b=/opt/hadoop/compose/ozonesecure-ha/data/$om_id/backup1 --index=2 -s=${logpath}"
  echo "Repair command executed for ${om_id}."
  docker start "${om_container}"
  echo "Container '${om_container}' started again."
  bucketTable=$(execute_command_in_container ${SCM} bash -c "ozone debug ldb --db=/opt/hadoop/compose/ozonesecure-ha/data/$om_id/metadata/om.db scan --cf=bucketTable")
  echo "Bucket table for ${om_id}:"
  if echo "$bucketTable" | grep -q "bucket-crash-1"; then
    echo "bucket 'bucket-crash-1' should not have been created, but it is present in the bucketTable of $om_id"
    exit 1
  else
    echo "bucket 'bucket-crash-1' is not present in the bucketTable of $om_id as expected."
  fi
}

echo "Testing ratis transaction repair on all OMs"
execute_robot_test ${SCM} kinit.robot
execute_robot_test ${SCM} repair/ratis-transaction-repair.robot
repair_and_restart_om "ozonesecure-ha-om1-1" "om1"
repair_and_restart_om "ozonesecure-ha-om2-1" "om2"
repair_and_restart_om "ozonesecure-ha-om3-1" "om3"
wait_for_om_leader
if ! execute_command_in_container scm1.org timeout 15s ozone sh volume list 1>/dev/null; then
  echo "Command timed out or failed => OMs are not running as expected. Test for repairing ratis transaction failed."
  exit 1
fi
echo "Testing ratis transaction repair completed successfully."

execute_robot_test ${OM} kinit.robot

echo "Creating test keys to verify om compaction"
om_container="ozonesecure-ha-om1-1"
docker exec "${om_container}" ozone freon ockg -n 1000 -t 4 -s 0 > /dev/null 2>&1
echo "Test keys created"

echo "Restarting OM after key creation to flush and generate sst files"
docker restart "${om_container}"
# Delete keys to create tombstones that need compaction
execute_command_in_container ${OM} ozone fs -rm -R -skipTrash ofs://${OM_SERVICE_ID}/vol1/bucket1

get_om_db_size() {
  execute_command_in_container ${OM} find /data/metadata/om.db -name '*.sst' -exec du -b {} + \
      | awk '{ sum += $1 } END { print sum + 0 }'
}

get_cf_entry_count() {
  local cf="$1"
  execute_command_in_container ${OM} bash -c \
      "ozone debug ldb --db=/data/metadata/om.db scan --cf=${cf} --count 2>/dev/null" \
      | tr -d '[:space:]'
}

wait_for_bucket_deletion_complete() {
  local timeout=300 n cf
  local cfs=(fileTable directoryTable deletedTable deletedDirectoryTable)
  SECONDS=0
  while [[ $SECONDS -lt $timeout ]]; do
    for cf in "${cfs[@]}"; do
      n=$(get_cf_entry_count "${cf}")
      [[ "${n:-1}" -eq 0 ]] || continue 2
    done
    return 0
    sleep 3
  done
  echo "Timed out waiting for bucket deletion to complete"
  return 1
}

wait_for_om_db_size_stable() {
  local timeout=180
  local stable_reads=0
  local required_stable_reads=3
  local prev=-1
  SECONDS=0
  while [[ $SECONDS -lt $timeout ]]; do
    local size
    size=$(get_om_db_size)
    if [[ ${size} -eq ${prev} ]]; then
      stable_reads=$((stable_reads + 1))
      if [[ ${stable_reads} -ge ${required_stable_reads} ]]; then
        return 0
      fi
    else
      stable_reads=0
      prev=${size}
    fi
    sleep 3
  done
  echo "Timed out waiting for OM DB size to stabilize"
  return 1
}

check_om_log() {
  docker-compose logs "${OM}" | grep "Compaction request for column family \"${1}\" completed"
}

compact_om_db() {
  for cf in "$@"; do
    execute_command_in_container ${OM} ozone repair om compact --cf="${cf}" --service-id "${OM_SERVICE_ID}" --node-id "${OM}" --blc kForce
    if ! RETRY_ATTEMPTS=20 retry check_om_log "$cf"; then
      echo "Compaction did not complete for column family ${cf}"
      return 1
    fi
  done
}

declare -i size_before_compaction size_after_compaction
wait_for_bucket_deletion_complete || exit 1
wait_for_om_db_size_stable || exit 1

size_before_compaction=$(get_om_db_size)
echo "OM DB SST size before compaction: ${size_before_compaction}"
compact_om_db fileTable directoryTable deletedTable deletedDirectoryTable || exit 1
wait_for_om_db_size_stable || exit 1
size_after_compaction=$(get_om_db_size)

echo "OM DB SST size after compaction: ${size_after_compaction}"
if (( size_after_compaction >= size_before_compaction )); then
  echo "OM DB size should be reduced after compaction. Before: ${size_before_compaction}, After: ${size_after_compaction}"
  exit 1
fi
