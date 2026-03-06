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

# This script tests upgrade from a previous release to the current
# binaries.  Docker image with Ozone binaries is required for the
# initial version, while the snapshot version uses Ozone runner image.

set -e -o pipefail

# Fail if required vars are not set.
set -u
: "${OZONE_UPGRADE_FROM}"
: "${OZONE_UPGRADE_TO}"
: "${TEST_DIR}"
: "${SCM}"
: "${OZONE_CURRENT_VERSION}"
set +u

echo "--- RUNNING ROLLING UPGRADE TEST FROM $OZONE_UPGRADE_FROM TO $OZONE_UPGRADE_TO ---"

source "$TEST_DIR"/testlib.sh

# Restart one service with the target image.
rolling_restart_service() {
  SERVICE="$1"

  echo "--- RESTARTING ${SERVICE} WITH IMAGE ${OZONE_UPGRADE_TO} ---"

  # Stop service
  stop_containers "${SERVICE}"

  # Check if this SCM container is running, as during a rolling upgrade it does stop-start one-by-one and
  # we want to run write/read tests while one service is unavailable. Choose SCM (the container where the generate and
  # validate robot tests are running) considering availability.
  if [[ "$(docker inspect -f '{{.State.Running}}' "ha-${SCM}-1" 2>/dev/null)" != "true" ]]; then
    local fallback_scm
    fallback_scm="$(docker-compose --project-directory="$TEST_DIR/compose/ha" config --services | grep scm | grep -v "^${SCM}$" | head -n1)"
    if [[ -n "$fallback_scm" ]]; then
      export SCM="$fallback_scm"
    fi
  fi

  # The data generation/validation is doing S3 API tests, so skip it in case the S3 gateway is updated
  # TODO find a better solution
  if [[ ${SERVICE} != "s3g" ]]; then
    callback before_service_restart
  fi

  # Restart service with new image.
  prepare_for_image "${OZONE_UPGRADE_TO}"
  create_containers "${SERVICE}"

  # The data generation/validation is doing S3 API tests, so skip it in case the S3 gateway is updated
  if [[ ${SERVICE} != "s3g" ]]; then
    callback after_service_restart
  fi

  # Service-specific readiness checks.
  case "${SERVICE}" in
    om*)
      wait_for_port "${SERVICE}" 9862 120
      ;;
    scm*)
      # SCM hostnames in this compose are scmX.org
      wait_for_port "${SERVICE}.org" 9876 120
      ;;
    dn*)
      wait_for_port "${SERVICE}" 9882 120
      ;;
  esac
}

echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-1-original"
export OM_HA_ARGS='--'
prepare_for_image "$OZONE_UPGRADE_FROM"

echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"
start_docker_env

# TODO Add old data generation

echo "--- ROLLING UPGRADE TO $OZONE_UPGRADE_TO PRE-FINALIZED ---"

# SCMs first
for s in scm2 scm1 scm3; do
  OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-${s}"
  rolling_restart_service "$s" "$OZONE_UPGRADE_TO"
done

# Recon
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-recon"
rolling_restart_service "recon" "$OZONE_UPGRADE_TO"

# DNs
for s in dn1 dn2 dn3 dn4 dn5; do
  OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-${s}"
  rolling_restart_service "$s" "$OZONE_UPGRADE_TO"
done

# OMs with upgrade arg
export OM_HA_ARGS='--upgrade'
for s in om1 om2 om3; do
  OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-${s}"
  rolling_restart_service "$s" "$OZONE_UPGRADE_TO"
done

# S3 Gateway
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-s3g"
rolling_restart_service "s3g" "$OZONE_UPGRADE_TO"

# TODO Add downgrade scenario

echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO FINALIZED ---"
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-3-finalized"

# TODO Add validation for pre-finalized state

# Sends commands to finalize OM and SCM.
execute_robot_test "$SCM" -N "${OUTPUT_NAME}-finalize" upgrade/finalize.robot
