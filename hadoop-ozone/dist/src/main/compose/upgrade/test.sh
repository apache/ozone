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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_UPGRADE_FROM:="0.5.0"}"
: "${OZONE_UPGRADE_TO:="1.0.0"}"
: "${OZONE_VOLUME:="${COMPOSE_DIR}/data"}"

export OZONE_VOLUME

mkdir -p "${OZONE_VOLUME}"/{dn1,dn2,dn3,om,recon,s3g,scm}

if [[ -n "${OZONE_VOLUME_OWNER}" ]]; then
  current_user=$(whoami)
  if [[ "${OZONE_VOLUME_OWNER}" != "${current_user}" ]]; then
    chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}" \
      || sudo chown -R "${OZONE_VOLUME_OWNER}" "${OZONE_VOLUME}"
  fi
fi

# define version-specifics
export OZONE_DIR=/opt/ozone
export OZONE_IMAGE="apache/ozone:${OZONE_UPGRADE_FROM}"
# shellcheck source=/dev/null
source "${COMPOSE_DIR}/versions/ozone-${OZONE_UPGRADE_FROM}.sh"
# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

# prepare pre-upgrade cluster
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/generate.robot
execute_robot_test scm -v PREFIX:pre freon/validate.robot
KEEP_RUNNING=false stop_docker_env

# run upgrade scripts
SCRIPT_DIR=../../libexec/upgrade
[[ -f "${SCRIPT_DIR}/${OZONE_UPGRADE_TO}.sh" ]] && "${SCRIPT_DIR}/${OZONE_UPGRADE_TO}.sh"

# update version-specifics
export OZONE_DIR=/opt/hadoop
unset OZONE_IMAGE # use apache/ozone-runner defined in docker-compose.yaml
# shellcheck source=/dev/null
source "${COMPOSE_DIR}/versions/ozone-${OZONE_UPGRADE_TO}.sh"
# shellcheck source=/dev/null
source "${COMPOSE_DIR}/../testlib.sh"

# re-start cluster with new version and check after upgrade
export OZONE_KEEP_RESULTS=true
start_docker_env
execute_robot_test scm -v PREFIX:pre freon/validate.robot
# test write key to old bucket after upgrade
execute_robot_test scm -v PREFIX:post freon/generate.robot
execute_robot_test scm -v PREFIX:post freon/validate.robot
stop_docker_env

generate_report
