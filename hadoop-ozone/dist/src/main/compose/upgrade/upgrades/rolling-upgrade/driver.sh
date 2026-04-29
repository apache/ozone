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
: "${CLIENT}"
: "${OZONE_CURRENT_VERSION}"
set +u

echo "--- RUNNING ROLLING UPGRADE TEST FROM $OZONE_UPGRADE_FROM TO $OZONE_UPGRADE_TO ---"

source "$TEST_DIR"/testlib.sh

# Restart one service with the target image.
rolling_restart_service() {
  SERVICE="$1"

  echo "--- RESTARTING ${SERVICE} WITH IMAGE ${TARGET_IMAGE} ---"

  # Stop service
  stop_containers "${SERVICE}"

  callback before_service_restart

  # Restart service with the requested image.
  prepare_for_image "${TARGET_IMAGE}"
  create_containers "${SERVICE}"

  callback after_service_restart

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
    s3g*)
      wait_for_port "${SERVICE}" 9878 120
      ;;
  esac
}

rolling_restart_all_services() {
  STAGE_PREFIX="$1"
  TARGET_IMAGE="$2"
  local s

  # SCMs first
  for s in scm2 scm1 scm3; do
    OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-${STAGE_PREFIX}-${s}"
    rolling_restart_service "$s"
  done

  # Recon
  OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-${STAGE_PREFIX}-recon"
  rolling_restart_service "recon"

  # DNs
  for s in dn1 dn2 dn3 dn4 dn5; do
    OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-${STAGE_PREFIX}-${s}"
    rolling_restart_service "$s"
  done

  # OMs
  for s in om1 om2 om3; do
    OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-${STAGE_PREFIX}-${s}"
    rolling_restart_service "$s"
  done

  # S3 Gateways (s3g is HAProxy and does not need to be upgraded)
  for s in s3g1 s3g2 s3g3; do
    OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-${STAGE_PREFIX}-${s}"
    rolling_restart_service "$s"
  done
}

echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-1-original"
prepare_for_image "$OZONE_UPGRADE_FROM"

echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"
start_docker_env

callback with_old_version

echo "--- ROLLING UPGRADE TO $OZONE_UPGRADE_TO PRE-FINALIZED ---"
rolling_restart_all_services "2-upgrade" "$OZONE_UPGRADE_TO"

OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-pre-finalized"
callback with_this_version_pre_finalized

echo "--- ROLLING DOWNGRADE TO $OZONE_UPGRADE_FROM ---"
rolling_restart_all_services "3-downgrade" "$OZONE_UPGRADE_FROM"

OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-3-downgraded"
callback with_old_version_downgraded

echo "--- ROLLING UPGRADE TO $OZONE_UPGRADE_TO ---"
rolling_restart_all_services "4-upgrade" "$OZONE_UPGRADE_TO"

# Upgrade client after all server components are upgraded but before finalization,
# so new client APIs can be exercised against pre-finalized servers.
echo "--- UPGRADING CLIENT TO $OZONE_UPGRADE_TO ---"
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-2-client"
stop_containers "$CLIENT"
prepare_for_image "${OZONE_UPGRADE_TO}"
create_containers "$CLIENT"

echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO FINALIZED ---"
OUTPUT_NAME="${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}-5-finalized"

# Sends commands to finalize OM and SCM.
execute_robot_test "$CLIENT" -N "${OUTPUT_NAME}-finalize" upgrade/finalize.robot
callback with_this_version_finalized
