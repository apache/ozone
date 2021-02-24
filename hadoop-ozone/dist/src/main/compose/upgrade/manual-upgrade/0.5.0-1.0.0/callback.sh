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

setup_old_version() {
  echo "--- SETTING UP OLD VERSION $OZONE_UPGRADE_FROM ---"
  load_version_specifics "$OZONE_UPGRADE_FROM"
}

with_old_version() {
  echo "--- RUNNING WITH OLD VERSION $OZONE_UPGRADE_FROM ---"

  generate old
  validate old
}

setup_new_version() {
  echo "--- SETTING UP NEW VERSION $OZONE_UPGRADE_TO ---"
  unload_version_specifics "$OZONE_UPGRADE_FROM"
  load_version_specifics "$OZONE_UPGRADE_TO"
}

with_new_version() {
  echo "--- RUNNING WITH NEW VERSION $OZONE_UPGRADE_TO ---"

  validate old
  generate new
  validate new
}
