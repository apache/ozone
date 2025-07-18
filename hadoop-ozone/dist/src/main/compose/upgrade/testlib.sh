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

# Cumulative result of all tests run with run_test function.
# 0 if all passed, 1 if any failed.
: "${RESULT:=0}"
: "${OZONE_REPLICATION_FACTOR:=3}"
: "${ALL_RESULT_DIR:="$_upgrade_dir"/result}"

# export for docker-compose
export OZONE_REPLICATION_FACTOR

source "${_upgrade_dir}/../testlib.sh"

## @description Prepares to run an image with `start_docker_env`.
## @param the version of Ozone to be run.
##   If this is equal to the string 'current', then the ozone runner image will
#    be used.
##   Else, a binary image will be used.
prepare_for_image() {
  local image_version="${1}"

  if [[ "$image_version" = "$OZONE_CURRENT_VERSION" ]]; then
      prepare_for_runner_image
  else
      prepare_for_binary_image "${image_version}"
  fi
}

## @description Run the common callback function first, then the one specific to
##   the upgrade being tested if one exists. If neither exists, print a
##   warning that nothing was tested.
## @param The name of the function to run.
callback() {
  local func="$1"

  set -u
  : "${OZONE_UPGRADE_CALLBACK}"
  : "${OZONE_COMMON_CALLBACK}"
  set +u

  (
    # Common callback always exists.
    source "$OZONE_COMMON_CALLBACK"
    if [[ "$(type -t "$func")" = function ]]; then
      "$func"
    fi
  )

  (
    # Version specific callback is optional.
    if [[ -f "$OZONE_UPGRADE_CALLBACK" ]]; then
      source "$OZONE_UPGRADE_CALLBACK"
      if [[ "$(type -t "$func")" = function ]]; then
        "$func"
      fi
    fi
  )
}

## @description Sets up and runs the upgrade test using the provided ozone
#     versions and docker compose cluster.
## @param The directory with a load.sh file that can be sourced to load the
#     docker compose cluster to run the test in.
## @param The directory of the upgrade type to run.
## @param The version of Ozone to upgrade from.
## @param The version of Ozone to upgrade to.
run_test() {
  local compose_cluster="$1"
  local upgrade_type="$2"
  export OZONE_UPGRADE_FROM="$3"
  export OZONE_UPGRADE_TO="$4"

  local test_dir="$_upgrade_dir/upgrades/$upgrade_type"
  local callback_dir="$test_dir"/callbacks
  local execution_dir="$test_dir"/execution/"$compose_cluster-$upgrade_type-${OZONE_UPGRADE_FROM}-${OZONE_UPGRADE_TO}"
  local compose_dir="$_upgrade_dir"/compose/"$compose_cluster"
  # Export variables needed by test, since it is run in a subshell.
  export OZONE_UPGRADE_CALLBACK="$callback_dir"/"$OZONE_UPGRADE_TO"/callback.sh
  export OZONE_COMMON_CALLBACK="$callback_dir"/common/callback.sh
  export OZONE_VOLUME="$execution_dir"/data
  export RESULT_DIR="$execution_dir"/result

  # Load docker compose setup.
  source "$compose_dir"/load.sh

  # The container to run test commands from. Use one of the SCM containers,
  # but SCM HA may or may not be used.
  export SCM="$(docker-compose --project-directory="$compose_dir" config --services | grep --max-count=1 scm)"

  if ! run_test_script "$test_dir" ./driver.sh; then
    RESULT=1
  fi

  copy_results "$execution_dir" "$ALL_RESULT_DIR"
}
