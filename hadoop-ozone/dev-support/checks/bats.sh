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
cd "${DIR}/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"${DIR}/../../../target/bats"}
mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/summary.txt"

source "${DIR}/_lib.sh"
source "${DIR}/install/bats.sh"

rm -f "${REPORT_DIR}/output.log"

find * \( \
    -path '*/src/test/shell/*' -name '*.bats' \
    -or -path dev-support/ci/selective_ci_checks.bats \
    -or -path dev-support/ci/pr_title_check.bats \
    -or -path dev-support/ci/find_test_class_project.bats \
    \) -print0 \
  | xargs -0 -n1 bats --formatter tap \
  | tee -a "${REPORT_DIR}/output.log"
rc=$?

grep '^\(not ok\|#\)' "${REPORT_DIR}/output.log" > "${REPORT_FILE}"

grep -c '^not ok' "${REPORT_FILE}" > "${REPORT_DIR}/failures"

ERROR_PATTERN=""
source "${DIR}/_post_process.sh"
