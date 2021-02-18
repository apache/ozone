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
set -x
COMPOSE_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
ALL_RESULT_DIR="$COMPOSE_DIR/result"
mkdir -p "$ALL_RESULT_DIR"
rm "$ALL_RESULT_DIR"/* || true
export COMPOSE_DIR
source "$COMPOSE_DIR/testlib.sh"
source "$COMPOSE_DIR/../testlib.sh"
OZONE_CURRENT_VERSION=1.1.0

RESULT=0
run_test() {
  local test_dir="$COMPOSE_DIR"/"$1"
  OZONE_UPGRADE_FROM="$2"
  OZONE_UPGRADE_TO="$3"

  local test_subdir="$test_dir"/"$OZONE_UPGRADE_FROM"-"$OZONE_UPGRADE_TO"

  OZONE_VOLUME="$test_subdir"/data
  create_data_dir
  RESULT_DIR="$test_subdir"/result
  source "$test_subdir"/callback.sh

  if ! run_test_script "${test_dir}"; then
    RESULT=1
  fi

  copy_results "${result_parent_dir}" "${ALL_RESULT_DIR}"
}

# Upgrade tests to be run:
run_test manual-upgrade 0.5.0 1.0.0
run_test non-rolling-upgrade 1.0.0 1.1.0

generate_report "upgrade" "${ALL_RESULT_DIR}"

exit ${RESULT}
