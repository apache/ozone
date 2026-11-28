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

check_snapshot_local_data() {
  local om
  for om in $(docker-compose config --services | grep '^om'); do
    execute_robot_test "$om" -N "${OUTPUT_NAME}-snapshot-local-data-${om}" -v COMPRESSED:"$1" upgrade/snapshot-local-data.robot
  done
}

with_this_version_pre_finalized() {
  # Snapshot local data YAML files must stay readable by the old version after downgrade.
  check_snapshot_local_data false
}

with_this_version_finalized() {
  # New files are written compressed, and files written before finalization are compressed in the background.
  check_snapshot_local_data true
}
