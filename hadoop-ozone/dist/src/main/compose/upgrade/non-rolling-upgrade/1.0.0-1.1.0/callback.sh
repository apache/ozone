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
  # OM preparation is not in 1.0.0.
  export OZONE_OM_PREPARE='false'
  load_version_specifics "$OZONE_UPGRADE_FROM"
}

with_old_version() {
   echo 'with old version'
}

with_new_version_pre_finalized() {
   echo 'with new version'
}

with_old_version_rollback() {
   echo 'with old version rollback'
}

with_new_version_finalized() {
   echo 'with new version finalized'
}
