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

source "$COMPOSE_DIR"/testlib.sh

setup() {
  echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"

  # OM preparation is not implemented until 1.2.0.
  export OZONE_OM_PREPARE='false'
  load_version_specifics "$OZONE_UPGRADE_FROM"
}

with_old_version() {
  echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"

  generate old1
  validate old1
}

with_new_version_pre_finalized() {
  echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO PRE-FINALIZED ---"

  validate old1

  generate new1
  validate new1
}

with_old_version_downgraded() {
  echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM AFTER DOWNGRADE ---"

  validate old1
  validate new1

  generate old2
  validate old2
}

with_new_version_finalized() {
  echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO FINALIZED ---"

  validate old1
  validate new1
  validate old2

  generate new2
  validate new2
}
