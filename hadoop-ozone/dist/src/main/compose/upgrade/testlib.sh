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

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"
: "${OZONE_VOLUME_OWNER:=}"

source "${_upgrade_dir}/../testlib.sh"

## @description Create the directory tree required for persisting data between
##   compose cluster restarts
create_data_dir() {
  if [[ -z "${OZONE_VOLUME}" ]]; then
    return 1
  fi

  rm -fr "${OZONE_VOLUME}" 2> /dev/null || sudo rm -fr "${OZONE_VOLUME}"
  mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}
  fix_data_dir_permissions
}

## @description Run upgrade steps required for going from one logical version to another.
## @param Starting logical version
## @param Target logical version
execute_upgrade_steps() {
  local -i from=$1
  local -i to=$2

  if [[ ${from} -ge ${to} ]]; then
    return
  fi

  pushd ${_testlib_dir}/../libexec/upgrade

  local v
  for v in $(seq ${from} $((to-1))); do
    if [[ -e "v$v.sh" ]]; then
      source "v$v.sh"
    fi
  done

  popd
}

## @description Pre-upgrade test steps
first_run() {
  start_docker_env
  execute_robot_test scm -v PREFIX:pre freon/generate.robot
  execute_robot_test scm -v PREFIX:pre freon/validate.robot
  KEEP_RUNNING=false stop_docker_env
}

## @description Post-upgrade test steps
second_run() {
  export OZONE_KEEP_RESULTS=true
  start_docker_env
  execute_robot_test scm -v PREFIX:pre freon/validate.robot
  # test write key to old bucket after upgrade
  execute_robot_test scm -v PREFIX:post freon/generate.robot
  execute_robot_test scm -v PREFIX:post freon/validate.robot
  stop_docker_env
}
