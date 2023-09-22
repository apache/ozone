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
# The test
#   * starts the docker environment with 'om3' inactive and uninitialised
#   * runs a robot test that creates keys and snapshots
#   * checks that 'om3' is inactive and has no data
#   * initialises 'om3'
#   * starts 'om3'
#   * verifies that 'om3' is running and is bootstrapping
#   * runs a robot test that validates the data on 'om3'
#
# The data creation robot test
#   * creates 100 metadata keys
#   * creates the first snapshot
#   * creates two actual keys and set the contents of each key, the same as the key name
#   * creates the second snapshot
#
# The data validation robot test
#   * checks that there have been checkpoints created on 'om3'
#   * once checkpoints are created, the 'om3' has all the data from the leader
#   * checks that 'om3' is not leader
#   * transfers leadership to 'om3', so that we can perform regular leader reads
#   * checks that the two snapshots exist on 'om3'
#   * runs a snapshot diff between the two snapshots
#   * validates that the result of the snapshot diff, contains just the two actual keys
#   * does a 'key cat' on both snapshot keys and validates the contents
#   * the keys are read from the snapshot and not the active file system
#   * the contents of each key should be the same as the key name

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

export SECURITY_ENABLED=true
export OM_SERVICE_ID="omservice"
export SCM=scm1.org
export COMPOSE_FILE=docker-compose.yaml:om-bootstrap.yaml

# shellcheck source=/dev/null
source "$COMPOSE_DIR/../testlib.sh"

start_docker_env

volume="vol1"
bucket="bucket1"
snap1="snap1"
snap2="snap2"
keyPrefix="sn"
key1="key1"
key2="key2"
bootstrap_om="om3"

execute_robot_test om1 kinit.robot

# Data creation
execute_robot_test om1 -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2} omha/data-creation-before-om-bootstrap.robot

echo "Check that om3 isn't running"
om3_service=$(execute_command_in_container om3 ps aux | grep 'OzoneManagerStarter' || true)

if [[ $om3_service != "" ]]
then
  echo "om3 is running, exiting..."
  exit 1
fi

echo "Check that om3 has no data"
om3_data=$(execute_command_in_container om3 ls -lah /data | grep 'metadata' || true)

if [[ $om3_data != "" ]]
then
  echo "om3 has data, exiting..."
  exit 1
fi

# Init om3 and start the om daemon in the background
execute_command_in_container om3 ozone om --init
execute_command_in_container -d om3 ozone om
wait_for_port om3 9872 120

echo "Check that om3 is running"
om3_service=$(execute_command_in_container om3 ps aux | grep 'OzoneManagerStarter' || true)

if [[ $om3_service == "" ]]
then
  echo "om3 isn't running, exiting..."
  exit 1
fi

echo "Check that om3 has data"
om3_data=$(execute_command_in_container om3 ls -lah /data | grep 'metadata' || true)

if [[ $om3_data == "" ]]
then
  echo "om3 has no data, exiting..."
  exit 1
fi

execute_robot_test om3 kinit.robot

# This test checks the disk on the node it's running. It needs to be run on om3.
execute_robot_test om3 -v BOOTSTRAPPED_OM:${bootstrap_om} -v VOLUME:${volume} -v BUCKET:${bucket} -v SNAP_1:${snap1} -v SNAP_2:${snap2} -v KEY_PREFIX:${keyPrefix} -v KEY_1:${key1} -v KEY_2:${key2} omha/data-validation-after-om-bootstrap.robot
