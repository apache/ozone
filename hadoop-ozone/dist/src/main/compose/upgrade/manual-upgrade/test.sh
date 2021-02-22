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
: "${COMPOSE_DIR}"
: "${OZONE_UPGRADE_CALLBACK}"
set +u

source "$COMPOSE_DIR"/testlib.sh
source "$OZONE_UPGRADE_CALLBACK"

prepare_for_image "$OZONE_UPGRADE_FROM"
callback setup_old_version
start_docker_env
callback with_old_version

stop_docker_env
callback setup_new_version

prepare_for_binary_image "$OZONE_UPGRADE_TO"
OZONE_KEEP_RESULTS=true start_docker_env
callback with_new_version

stop_docker_env
