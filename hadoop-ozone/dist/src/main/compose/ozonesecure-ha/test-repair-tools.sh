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
docker exec "${om_container}" ozone freon ockg -n 100000 -t 20 -s 0 > /dev/null 2>&1
echo "Test keys created"

echo "Restarting OM after key creation to flush and generate sst files"
docker restart "${om_container}"

execute_robot_test ${OM} repair/om-compact.robot
