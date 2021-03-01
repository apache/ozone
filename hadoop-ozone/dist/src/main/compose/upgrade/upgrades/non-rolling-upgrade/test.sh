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

echo "--- RUNNING NON-ROLLING UPGRADE TEST FROM $OZONE_UPGRADE_FROM TO $OZONE_UPGRADE_TO ---"

# Prepare OMs before upgrade unless this variable is false.
: "${OZONE_PREPARE_OMS:='true'}"

# Fail if required vars are not set.
set -u
: "${OZONE_UPGRADE_FROM}"
: "${OZONE_UPGRADE_TO}"
: "${TEST_DIR}"
: "${OZONE_UPGRADE_CALLBACK}"
set +u

# Tells main testlib.sh to wait for an OM leader.
export OM_SERVICE_ID=omservice
export COMPOSE_FILE="${TEST_DIR}/compose/ha/docker-compose.yaml"

source "$TEST_DIR"/testlib.sh
source "$OZONE_UPGRADE_CALLBACK"

prepare_oms() {
  if [[ "$OZONE_PREPARE_OMS" = 'true' ]]; then
    execute_robot_test scm upgrade/prepare.robot
  fi
}

echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"
callback setup

export OM_HA_ARGS='--'
prepare_for_image "$OZONE_UPGRADE_FROM"
start_docker_env
echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"
callback with_old_version
prepare_oms

prepare_for_image "$OZONE_CURRENT_VERSION"
export OM_HA_ARGS='--upgrade'
restart_docker_env
echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO PRE-FINALIZED ---"
callback with_new_version_pre_finalized
prepare_oms

prepare_for_image "$OZONE_UPGRADE_FROM"
export OM_HA_ARGS='--downgrade'
restart_docker_env
echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM AFTER DOWNGRADE ---"
callback with_old_version_downgraded
prepare_oms

prepare_for_image "$OZONE_CURRENT_VERSION"
export OM_HA_ARGS='--upgrade'
restart_docker_env
execute_robot_test scm upgrade/finalize.robot
echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO FINALIZED ---"
callback with_new_version_finalized

stop_docker_env
generate_report
