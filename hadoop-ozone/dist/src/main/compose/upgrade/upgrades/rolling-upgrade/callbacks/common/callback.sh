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

### CALLBACKS ###

## @description Run while a service is stopped, it'll generate data on the cluster (upgrade/generate.robot)
## @param The prefix to use for data generated.
with_service_stopped() {
  generate "${1}" "$CLIENT"
}

## @description Run while a service is started again (with new image), it'll validate data on the cluster (upgrade/validate.robot)
## @param The prefix to use for data generated.
with_service_restarted() {
  validate "${1}" "$CLIENT"
}

with_old_version() {
  execute_robot_test "$CLIENT" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  generate old1 "$CLIENT"
  validate old1 "$CLIENT"

  generate overwrite "$CLIENT"
}

with_this_version_pre_finalized() {
  # No check for pre-finalized status here, because the release may not have
  # added layout features to OM or HDDS.
  validate old1 "$CLIENT"

  generate new1 "$CLIENT"
  validate new1 "$CLIENT"

  validate overwrite "$CLIENT"
  generate overwrite "$CLIENT" --exclude create-volume-and-bucket
}

with_old_version_downgraded() {
  execute_robot_test "$CLIENT" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  validate old1 "$CLIENT"
  validate new1 "$CLIENT"

  generate old2 "$CLIENT"
  validate old2 "$CLIENT"

  validate overwrite "$CLIENT"
  generate overwrite "$CLIENT" --exclude create-volume-and-bucket
}

with_this_version_finalized() {
  execute_robot_test "$CLIENT" -N "${OUTPUT_NAME}-check-finalization" --include finalized upgrade/check-finalization.robot
  validate old1 "$CLIENT"
  validate new1 "$CLIENT"
  validate old2 "$CLIENT"

  generate new2 "$CLIENT"
  validate new2 "$CLIENT"

  validate overwrite "$CLIENT"
}
