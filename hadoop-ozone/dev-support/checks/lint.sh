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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

BASE_DIR="$(pwd -P)"
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/lint"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"

declare -i rc

if [[ -d "${BASE_DIR}/ozone-ui/src" ]]; then
  (
    cd "${BASE_DIR}/ozone-ui/src" || exit 1
    # Install dependencies if node_modules doesn't exist
    if [[ ! -d "node_modules" ]]; then
      pnpm install --frozen-lockfile
    fi
    pnpm run lint
  ) > "${REPORT_DIR}/output.log" 2>&1
  rc=$?
  
  cat "${REPORT_DIR}/output.log"
  
  # Parse lint output to report file (format: path:line:col: error message)
  # ::: Sample error output :::
  # 39:36  error  Insert `,`  prettier/prettier
  if [[ ${rc} -ne 0 ]]; then
    grep "^/" "${REPORT_DIR}/output.log" | grep ":.*error" > "$REPORT_FILE" || true
  fi
else
  rc=0
  echo "ozone-ui/src not found. Skipping UI lint."
fi

ERROR_PATTERN="error"
source "${DIR}/_post_process.sh"
