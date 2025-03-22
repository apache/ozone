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

with_this_version_pre_finalized() {
  # New layout features were added in this version, so OM and SCM should be pre-finalized.
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-check-finalization" --include pre-finalized upgrade/check-finalization.robot
  # Test that HSync is disabled when pre-finalized.
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-hsync" --include pre-finalized-hsync-tests hsync/upgrade-hsync-check.robot
}

with_this_version_finalized() {
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-hsync" admincli/lease-recovery.robot
  execute_robot_test "$SCM" -N "${OUTPUT_NAME}-freon-hsync" freon/hsync.robot
}
