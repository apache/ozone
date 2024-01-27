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

source "$TEST_DIR"/testlib.sh

### HELPER METHODS ###

## @description Generates data on the cluster.
## @param The prefix to use for data generated.
## @param All parameters after the first one are passed directly to the robot command,
##        see https://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#all-command-line-options
generate() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-generate-${1}" -v PREFIX:"$1" ${@:2} upgrade/generate.robot
}

## @description Validates that data exists on the cluster.
## @param The prefix of the data to be validated.
## @param All parameters after the first one are passed directly to the robot command,
##        see https://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#all-command-line-options
validate() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-validate-${1}" -v PREFIX:"$1" ${@:2} upgrade/validate.robot
}

### CALLBACKS ###

with_old_version() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  generate old1
  validate old1
}

with_this_version_pre_finalized() {
  # No check for pre-finalized status here, because the release may not have
  # added layout features to OM or HDDS.
  validate old1
  # HDDS-6261: overwrite the same keys intentionally
  generate old1 --exclude create-volume-and-bucket

  generate new1
  validate new1
}

with_old_version_downgraded() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  validate old1
  validate new1

  generate old2
  validate old2

  # HDDS-6261: overwrite the same keys again to trigger the precondition check
  # that exists <= 1.1.0 OM
  generate old1 --exclude create-volume-and-bucket
}

with_this_version_finalized() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  validate old1
  validate new1
  validate old2

  generate new2
  validate new2
}
