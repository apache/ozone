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

# Fail if required variables are not set.
set -u
: "${OZONE_CURRENT_VERSION}"
set +u

_upgrade_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Cumulative result of all tests run with run_test function.
# 0 if all passed, 1 if any failed.
: "${RESULT:=0}"
: "${OZONE_REPLICATION_FACTOR:=3}"
: "${OZONE_VOLUME_OWNER:=}"
: "${OZONE_CURRENT_VERSION:=}"
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
##   If this is equal to `OZONE_CURRENT_VERSION`, then the ozone runner image wil be used.
##   Else, a binary image will be used.
prepare_for_image() {
  local image_version="$1"

  if [[ "$image_version" = "$OZONE_CURRENT_VERSION" ]]; then
      prepare_for_runner_image
  else
      prepare_for_binary_image "$image_version"
  fi
}

## @description Runs a callback function only if it exists.
## @param The name of the function to run.
callback() {
  local func="$1"
  if [[ "$(type -t "$func")" = function ]]; then
    "$func"
  else
    echo "Skipping callback $func. No function implementation found."
  fi
}

## @description Sets up and runs the test defined by "$1"/test.sh.
## @param The directory for the upgrade type whose test.sh file will be run.
## @param The version of Ozone to upgrade from.
## @param The version of Ozone to upgrade to.
run_test() {
  # Export variables needed by test, since it is run in a subshell.
  local test_dir="$_upgrade_dir/upgrades/$1"
  export OZONE_UPGRADE_FROM="$2"
  export OZONE_UPGRADE_TO="$3"
  local test_subdir="$test_dir"/"$OZONE_UPGRADE_FROM"-"$OZONE_UPGRADE_TO"
  export OZONE_UPGRADE_CALLBACK="$test_subdir"/callback.sh
  export OZONE_VOLUME="$test_subdir"/data
  export RESULT_DIR="$test_subdir"/result

  if ! run_test_script "$test_dir" ./driver.sh; then
    RESULT=1
  fi

  generate_report 'upgrade' "$RESULT_DIR"
  copy_results "$test_subdir" "$ALL_RESULT_DIR"
}

## @description Generates data on the cluster.
## @param The prefix to use for data generated.
generate() {
    execute_robot_test scm -v PREFIX:"$1" upgrade/generate.robot
}

## @description Validates that data exists on the cluster.
## @param The prefix of the data to be validated.
validate() {
    execute_robot_test scm -v PREFIX:"$1" upgrade/validate.robot
}

## @description Checks that the metadata layout version of the provided node matches what is expected.
## @param The name of the docker-compose service to run the check on.
## @param The path to the VERSION file in the container.
## @param The metadata layout version expected for that service.
check_mlv() {
    service="$1"
    container_id="$(docker container ps --quiet --filter "name=$service")"

    # If some containers go down during the test run due to resources issues,
    # just print a message instead of failing the test.
    if  [[ -n "$container_id" ]]; then
      execute_robot_test "$service" -v VERSION_FILE:"$2" -v VERSION:"$3" upgrade/check-mlv.robot
    else
      echo "No matching containers for docker-compose service $service found. Skipping MLV check."
    fi
}

## @description Checks that the metadata layout version of a datanode matches what is expected.
## @param The name of the docker-compose service to run the check on.
## @param The metadata layout version expected for that service.
check_dn_mlv() {
  check_mlv "$1" /data/metadata/dnlayoutversion/VERSION "$2"
}

## @description Checks that the metadata layout version of an OM matches what is expected.
## @param The name of the docker-compose service to run the check on.
## @param The metadata layout version expected for that service.
check_om_mlv() {
  check_mlv "$1" /data/metadata/om/current/VERSION "$2"
}

## @description Checks that the metadata layout version of an SCM matches what is expected.
## @param The name of the docker-compose service to run the check on.
## @param The metadata layout version expected for that service.
check_scm_mlv() {
  check_mlv "$1" /data/metadata/scm/current/VERSION "$2"
}
