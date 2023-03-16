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
: "${OZONE_VOLUME_OWNER:=}"
: "${ALL_RESULT_DIR:="$_upgrade_dir"/result}"

# export for docker-compose
export OZONE_REPLICATION_FACTOR

source "${_upgrade_dir}/../testlib.sh"

## @description Create the directory tree required for persisting data between
##   compose cluster restarts
create_data_dirs() {
  local dirs_to_create="$@"

  if [[ -z "${OZONE_VOLUME}" ]]; then
    return 1
  fi

  rm -fr "${OZONE_VOLUME}" 2> /dev/null || sudo rm -fr "${OZONE_VOLUME}"
  mkdir -p $dirs_to_create
  fix_data_dir_permissions
}

## @description Prepares to run an image with `start_docker_env`.
## @param the version of Ozone to be run.
##   If this is equal to the string 'current', then the ozone runner image will
#    be used.
##   Else, a binary image will be used.
prepare_for_image() {
  local image_version="$1"

# Need a way to get the setup file for the current cluster.
  source "$(get_cluster_setup_file)"

  if [[ "$image_version" = 'current' ]]; then
      prepare_for_runner_image
  else
      prepare_for_binary_image "$image_version"
  fi
}

## @description Prints the path to the file to source to load the docker
##    compose setup for this test.
## @param The name of the function to run.
get_cluster_setup_file() {
  set -u
  : "${OZONE_UPGRADE_CALLBACK}"
  : "${TEST_DIR}"
  set +u

  # Check if there is a callback to load a specific docker cluster.
  # Use the default one only if one is not defined.
  has_setup_override=false
  if [[ -f "$OZONE_UPGRADE_CALLBACK" ]]; then
    _run_callback "$OZONE_UPGRADE_CALLBACK" get_cluster_setup_file && has_setup_override=true
  fi

  if [[ "$has_setup_override" = false ]]; then
    _run_callback "$TEST_DIR"/upgrades/"$UPGRADE_TYPE"/common/callback.sh get_cluster_setup_file
  fi
}

## @description Runs a callback function only if it exists.
## @param The name of the function to run.
callback() {
  local func="$1"

  set -u
  : "${OZONE_UPGRADE_CALLBACK}"
  : "${TEST_DIR}"
  set +u

  # If this upgrade has no callbacks for custom testing, only the common ones
  # will be run.
  if [[ -f "$OZONE_UPGRADE_CALLBACK" ]]; then
    _run_callback "$OZONE_UPGRADE_CALLBACK" "$func"
  fi
  _run_callback "$TEST_DIR"/upgrades/non-rolling-upgrade/common/callback.sh "$func"
}

_run_callback() {
  local script="$1"
  local func="$2"

  (
    if [[ -f "$script" ]]; then
      source "$script"
      if [[ "$(type -t "$func")" = function ]]; then
        "$func"
      else
        echo "Skipping callback $func. No function implementation found." 1>&2
        return 1
      fi
    else
        echo "Skipping callback $func. No script $script found." 1>&2
        return 1
    fi
  )
}

## @description Sets up and runs the test defined by "$1"/test.sh.
## @param The directory for the upgrade type whose test.sh file will be run.
## @param The version of Ozone to upgrade from.
## @param The version of Ozone to upgrade to.
run_test() {
  export UPGRADE_TYPE="$1"
  export OZONE_UPGRADE_FROM="$2"
  export OZONE_UPGRADE_TO="$3"

  # Export variables needed by test, since it is run in a subshell.
  local test_dir="$_upgrade_dir/upgrades/$UPGRADE_TYPE"
  local test_subdir="$test_dir"/"$OZONE_UPGRADE_FROM"
  export OZONE_UPGRADE_CALLBACK="$test_subdir"/callback.sh
  export OZONE_VOLUME="$test_subdir"/"$OZONE_UPGRADE_TO"/data
  export RESULT_DIR="$test_subdir"/"$OZONE_UPGRADE_TO"/result

  if ! run_test_script "$test_dir" ./driver.sh; then
    RESULT=1
  fi

  copy_results "$test_subdir" "$ALL_RESULT_DIR"
}
