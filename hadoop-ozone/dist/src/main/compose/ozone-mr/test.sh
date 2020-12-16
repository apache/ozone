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
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
ALL_RESULT_DIR="$SCRIPT_DIR/result"
mkdir -p "$ALL_RESULT_DIR"
rm "$ALL_RESULT_DIR/*" || true
source "$SCRIPT_DIR/../testlib.sh"

tests=$(find_tests)
cd "$SCRIPT_DIR"

RESULT=0
# shellcheck disable=SC2044
for t in ${tests}; do
  d="$(dirname "${t}")"

  if ! run_test_script "${d}"; then
    RESULT=1
  fi

  copy_results "${d}" "${ALL_RESULT_DIR}"
done

exit ${RESULT}
