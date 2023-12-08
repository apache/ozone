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

# This script tests upgrade from one release to a later one.  Docker
# image with Ozone binaries are required for both versions.

set -e -o pipefail

# Fail if required vars are not set.
set -u
: "${OZONE_UPGRADE_FROM}"
: "${OZONE_UPGRADE_TO}"
: "${TEST_DIR}"
: "${OZONE_UPGRADE_CALLBACK}"
set +u

source "$TEST_DIR"/compose/non-ha/load.sh
source "$TEST_DIR"/testlib.sh
[[ -f "$OZONE_UPGRADE_CALLBACK" ]] && source "$OZONE_UPGRADE_CALLBACK"

echo "--- RUNNING MANUAL UPGRADE TEST FROM $OZONE_UPGRADE_FROM TO $OZONE_UPGRADE_TO ---"

echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"
OUTPUT_NAME="$OZONE_UPGRADE_FROM"
prepare_for_image "$OZONE_UPGRADE_FROM"
callback setup_old_version

echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"
start_docker_env
callback with_old_version
stop_docker_env

echo "--- SETTING UP NEW VERSION $OZONE_UPGRADE_TO ---"
OUTPUT_NAME="$OZONE_UPGRADE_TO"
prepare_for_image "$OZONE_UPGRADE_TO"
callback setup_this_version

echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO ---"
OZONE_KEEP_RESULTS=true start_docker_env
callback with_this_version
