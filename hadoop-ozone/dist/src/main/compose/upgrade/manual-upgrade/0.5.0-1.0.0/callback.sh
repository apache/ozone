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
  OZONE_SAFEMODE_STATUS_COMMAND='ozone scmcli safemode status'
}

# with_old_version() { }

setup_new_version() {
  OZONE_SAFEMODE_STATUS_COMMAND='ozone admin safemode status --verbose'

  local v100_libexec="$COMPOSE_DIR"/../libexec/upgrade/1.0.0
  for script in "$v100_libexec"/*; do
    "$script"
  done
}

# with_new_version() { }
