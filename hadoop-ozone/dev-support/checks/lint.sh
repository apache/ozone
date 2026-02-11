#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#checks:basic

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

BASE_DIR="$(pwd -P)"
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/lint"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/lint-summary.txt"

declare -i rc

if [[ -d "${BASE_DIR}/ozone-ui/src" ]]; then
  (
    cd "${BASE_DIR}/ozone-ui/src" || exit 1
    # Install dependencies if node_modules doesn't exist
    if [[ ! -d "node_modules" ]]; then
      pnpm install --frozen-lockfile
    fi
    pnpm run lint
  ) 2>&1 | tee "${REPORT_DIR}/output.log"
  rc=${PIPESTATUS[0]}
  
  # Parse lint output to report file
  if [[ ${rc} -ne 0 ]]; then
    grep -v "^>" "${REPORT_DIR}/output.log" | grep ":" > "$REPORT_FILE" || true
    if [[ ! -s "$REPORT_FILE" ]]; then
      echo "UI lint failed. See ${REPORT_DIR}/output.log for details." > "$REPORT_FILE"
    fi
  else
    touch "$REPORT_FILE"
  fi
else
  rc=0
  echo "ozone-ui/src not found. Skipping UI lint." | tee "$REPORT_FILE"
fi

exit ${rc}
