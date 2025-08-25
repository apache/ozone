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

#suite:HA-secure

# This test aims to validate the ozone snapshot data that have been
# installed on a bootstrapped OM after a Ratis snapshot installation.
#
# This test starts 'om3' as FOLLOWER and 'om4' as LISTENER.
#
# The test
#   * starts the docker environment with 'om' inactive and uninitialised
#   * runs a robot test that creates keys and snapshots
#   * checks that 'om' is inactive and has no data
#   * initialises 'om'
#   * starts 'om'
#   * verifies that 'om' is running and is bootstrapping
#   * runs a robot test that validates the data on 'om'
#
# The data creation robot test
#   * creates 100 metadata keys
#   * creates the first snapshot
#   * creates two actual keys and set the contents of each key, the same as the key name
#   * creates the second snapshot
#
# The data validation robot test
#   * checks that there have been checkpoints created on 'om'
#   * once checkpoints are created, the 'om' has all the data from the leader
#   * checks that 'om' is not leader
#   * transfers leadership to 'om', so that we can perform regular leader reads
#   * checks that the two snapshots exist on 'om'
#   * runs a snapshot diff between the two snapshots
#   * validates that the result of the snapshot diff, contains just the two actual keys
#   * does a 'key cat' on both snapshot keys and validates the contents
#   * the keys are read from the snapshot and not the active file system
#   * the contents of each key should be the same as the key name

set -u -o pipefail

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:om-bootstrap.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

# Function to check and bootstrap an OM node
# Usage: check_and_bootstrap_om <om_node_name> <bootstrapped_om_param> [is_follower_param]
check_and_bootstrap_om() {
  local om_node_name="$1"
  local bootstrapped_om_param="$2"
  local is_follower_param="${3:-}"
  
  echo "Check that ${om_node_name} isn't running"
  local om_service
  om_service=$(execute_command_in_container "${om_node_name}" ps aux | grep 'OzoneManagerStarter' || true)

  if [[ $om_service != "" ]]
  then
    echo "${om_node_name} is running, exiting..."
    exit 1
  fi

  echo "Check that ${om_node_name} has no data"
  local om_data
  om_data=$(execute_command_in_container "${om_node_name}" ls -lah /data | grep 'metadata' || true)

  if [[ $om_data != "" ]]
  then
    echo "${om_node_name} has data, exiting..."
    exit 1
  fi

  # Init ${om_node_name} and start the om daemon in the background
  execute_command_in_container "${om_node_name}" ozone om --init
  execute_command_in_container -d "${om_node_name}" ozone om
  wait_for_port "${om_node_name}" 9872 120

  echo "Check that ${om_node_name} is running"
  om_service=$(execute_command_in_container "${om_node_name}" ps aux | grep 'OzoneManagerStarter' || true)

  if [[ $om_service == "" ]]
  then
    echo "${om_node_name} isn't running, exiting..."
    exit 1
  fi

  echo "Check that ${om_node_name} has data"
  om_data=$(execute_command_in_container "${om_node_name}" ls -lah /data | grep 'metadata' || true)

  if [[ $om_data == "" ]]
  then
    echo "${om_node_name} has no data, exiting..."
    exit 1
  fi

  execute_robot_test "${om_node_name}" kinit.robot

  # Build robot test parameters
  local robot_params="-v BOOTSTRAPPED_OM:${bootstrapped_om_param} -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2}"
  
  # Add IS_FOLLOWER parameter if provided
  if [[ -n "${is_follower_param}" ]]; then
    robot_params="${robot_params} -v IS_FOLLOWER:${is_follower_param}"
  fi

  # This test checks the disk on the node it's running. It needs to be run on the specified OM node.
  execute_robot_test "${om_node_name}" ${robot_params} omha/data-validation-after-om-bootstrap.robot
}

start_docker_env

volume="vol1"
bucket="bucket1"
snap1="snap1"
snap2="snap2"
keyPrefix="sn"
key1="key1"
key2="key2"
bootstrap_om="om3"
bootstrap_listener="om4"

execute_robot_test om1 kinit.robot

# Data creation
execute_robot_test om1 -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2} omha/data-creation-before-om-bootstrap.robot

# Bootstrap om3 (FOLLOWER)
check_and_bootstrap_om "${bootstrap_om}" "${bootstrap_om}"

# Bootstrap om4 (LISTENER)
check_and_bootstrap_om "${bootstrap_listener}" "${bootstrap_listener}" "false"
