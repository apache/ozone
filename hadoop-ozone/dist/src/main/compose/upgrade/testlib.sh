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

set -e -o pipefail

_upgrade_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# The version that corresponds to the current build.
: "${OZONE_CURRENT_VERSION:=1.1.0}"
# Cumulative result of all tests run with run_test function.
# 0 if all passed, 1 if any failed.
: "${RESULT:=0}"
: "${OZONE_REPLICATION_FACTOR:=3}"
# TODO: Compose dir must be set or remove this.
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"
: "${OZONE_VOLUME_OWNER:=}"
: "${ALL_RESULT_DIR:="$_upgrade_dir"/result}"

# export for docker-compose
export OZONE_VOLUME
export OZONE_REPLICATION_FACTOR

source "${_upgrade_dir}/../testlib.sh"

## @description Create the directory tree required for persisting data between
##   compose cluster restarts
create_data_dir() {
  if [[ -z "${OZONE_VOLUME}" ]]; then
    return 1
  fi

  rm -fr "${OZONE_VOLUME}" 2> /dev/null || sudo rm -fr "${OZONE_VOLUME}"
  # TODO: Add HA to docker compose on upgrade branch.
  # mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om1,om2,om3,recon,s3g,scm}
  mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}
  fix_data_dir_permissions
}

prepare_for_image() {
    local image_version="$1"

    if [[ "$image_version" = "$OZONE_CURRENT_VERSION" ]]; then
        prepare_for_runner_image "$image_version"
    else
        prepare_for_binary_image "$image_version"
    fi
}

callback() {
    local func="$1"
    type -t "$func" > /dev/null && "$func"
}

run_test() {
  # Export variables needed by test, since it is run in a subshell.
  local test_dir="$COMPOSE_DIR"/"$1"
  export OZONE_UPGRADE_FROM="$2"
  export OZONE_UPGRADE_TO="$3"
  local test_subdir="$test_dir"/"$OZONE_UPGRADE_FROM"-"$OZONE_UPGRADE_TO"
  export OZONE_UPGRADE_CALLBACK="$test_subdir"/callback.sh

  OZONE_VOLUME="$test_subdir"/data
  export RESULT_DIR="$test_subdir"/result

  create_data_dir

  if ! run_test_script "${test_dir}"; then
    RESULT=1
  fi

  generate_report 'upgrade' "$RESULT_DIR"
  copy_results "$test_subdir" "${ALL_RESULT_DIR}"
}
