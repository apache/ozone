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

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# from <ozone-root>/hadoop-ozone/dev-support/checks to <ozone-root>
cd "${DIR}/../../.." || exit 1
BASE_DIR="$(pwd -P)"

CHECK=pyunit

REPORT_DIR=${OUTPUT_DIR:-"${BASE_DIR}/target/${CHECK}"}
mkdir -p "${REPORT_DIR}"

REPORT_FILE="${REPORT_DIR}/summary.txt"

python3 -m unittest discover -v -s dev-support/ci -p 'test_*.py' 2>&1 \
  | tee "${REPORT_DIR}/output.log"
rc=$?

grep '^\(FAIL\|ERROR\): ' "${REPORT_DIR}/output.log" > "${REPORT_FILE}"
grep -c '^\(FAIL\|ERROR\): ' "${REPORT_FILE}" > "${REPORT_DIR}/failures"

ERROR_PATTERN=""
source "${DIR}/_post_process.sh"
