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

# Helper function, not a callback.
_check_hdds_mlvs() {
  mlv="$1"
  check_scm_mlv scm "$mlv"
  check_dn_mlv dn1 "$mlv"
  check_dn_mlv dn2 "$mlv"
  check_dn_mlv dn3 "$mlv"
}

# Helper function, not a callback.
_check_om_mlvs() {
  mlv="$1"
  check_om_mlv om1 "$mlv"
  check_om_mlv om2 "$mlv"
  check_om_mlv om3 "$mlv"
}

setup() {
  # OM preparation is not implemented until 1.2.0.
  export OZONE_OM_PREPARE='false'
}

with_old_version() {
  generate old1
  validate old1
}

with_new_version_pre_finalized() {
  _check_hdds_mlvs 0
  _check_om_mlvs 0

  validate old1

  generate new1
  validate new1
}

with_old_version_downgraded() {
  validate old1
  validate new1

  generate old2
  validate old2
}

with_new_version_finalized() {
  _check_hdds_mlvs 3
  # OM currently only has one layout version.
  _check_om_mlvs 0

  validate old1
  validate new1
  validate old2

  generate new2
  validate new2
}
