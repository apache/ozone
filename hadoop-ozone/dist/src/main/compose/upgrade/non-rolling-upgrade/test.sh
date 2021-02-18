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

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_CURRENT_VERSION:="1.1.0"}"
: "${OZONE_PREPARE_OMS:=}"
: "${OZONE_UPGRADE_FROM:="0.5.0"}"
: "${OZONE_UPGRADE_TO:="$OZONE_CURRENT_VERSION"}"

prepare_for_image "$OZONE_UPGRADE_FROM"
start_docker_env
run_upgrade_hook with_old_version
if [[ "$OZONE_PREPARE_OMS" ]]; then
    execute_robot_test upgrade/om-prepare.robot
fi

prepare_for_image "$OZONE_CURRENT_VERSION"
restart_docker_env
run_upgrade_hook with_new_versino_pre_finalized

prepare_for_image "$OZONE_UPGRADE_FROM"
restart_docker_env
run_upgrade_hook with_old_version_rollback

prepare_for_image "$OZONE_CURRENT_VERSION"
restart_docker_env
execute_robot_test upgrade/finalize-scm.robot
execute_robot_test upgrade/finalize-om.robot
run_upgrade_hook with_new_version_finalized

stop_docker_env
generate_report

