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
  export OZONE_OM_PREPARE='true'
}

with_old_version() {
  generate old1
  validate old1
}

with_new_version_pre_finalized() {
  _check_hdds_mlvs 2
  _check_om_mlvs 0

  validate old1
#   HDDS-6261: overwrite the same keys intentionally
  generate old1 --exclude create-volume-and-bucket

  generate new1
  validate new1

  check_ec_is_disabled
}

with_old_version_downgraded() {
  validate old1
  validate new1

  generate old2
  validate old2

  # HDDS-6261: overwrite the same keys again to trigger the precondition check
  # that exists <= 1.1.0 OM
  generate old1 --exclude create-volume-and-bucket
}

with_new_version_finalized() {
  _check_hdds_mlvs 4
  _check_om_mlvs 3

  validate old1
  validate new1
  validate old2

  generate new2
  validate new2

  check_ec_is_enabled
}
