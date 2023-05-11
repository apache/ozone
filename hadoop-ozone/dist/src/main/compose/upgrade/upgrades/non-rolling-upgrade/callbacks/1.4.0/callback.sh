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

## @description Validates that if cluster supports snapshot feature.
## @param Whether the snapshot feature should be supported in cluster.
## @param All parameters after the first one are passed directly to the robot command,
##        see https://robotframework.org/robotframework/latest/RobotFrameworkUserGuide.html#all-command-line-options
validate_snapshot_support() {
  if [ "$1" = "true" ]; then
    TEST_TAG="snapshot-enabled"
  else
    TEST_TAG="snapshot-disabled"
  fi
  execute_robot_test "$SCM" --include "${TEST_TAG}" upgrade/snapshot.robot
}

### CALLBACKS ###

with_old_version() {
  execute_robot_test "$SCM" --include finalized upgrade/check-finalization.robot
  validate_snapshot_support false
}

with_this_version_pre_finalized() {
  execute_robot_test "$SCM" --include pre-finalized upgrade/check-finalization.robot
  validate_snapshot_support false
}

with_old_version_downgraded() {
  execute_robot_test "$SCM" --include finalized upgrade/check-finalization.robot
  validate_snapshot_support false
}

with_this_version_finalized() {
  execute_robot_test "$SCM" --include finalized upgrade/check-finalization.robot
  validate_snapshot_support true
}

