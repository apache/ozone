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

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_CURRENT_VERSION:="1.1.0"}"
: "${OZONE_UPGRADE_FROM:="0.5.0"}"
: "${OZONE_UPGRADE_TO:="$OZONE_CURRENT_VERSION"}"

prepare_for_image "${OZONE_UPGRADE_FROM}"
# Load version specifics
callback setup_old_version
start_docker_env
callback with_old_version

stop_docker_env

# Unload version specifics, execute upgrade steps, load version specifics.
# from=$(get_logical_version "${OZONE_UPGRADE_FROM}")
# to=$(get_logical_version "${OZONE_UPGRADE_TO}")
# execute_upgrade_steps "$from" "$to"
callback setup_new_version

prepare_for_binary_image "${OZONE_UPGRADE_TO}"
OZONE_KEEP_RESULTS=true start_docker_env
callback with_new_version

stop_docker_env
generate_report
