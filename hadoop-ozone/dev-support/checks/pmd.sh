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

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/pmd"}
mkdir -p "$REPORT_DIR"

REPORT_FILE="$REPORT_DIR/summary.txt"

MAVEN_OPTIONS='-B -fae --no-transfer-progress'

declare -i rc

#shellcheck disable=SC2086
mvn $MAVEN_OPTIONS test-compile pmd:check -Dpmd.failOnViolation=false -Dpmd.printFailingErrors "$@" > "${REPORT_DIR}/pmd-output.log"

rc=$?

cat "${REPORT_DIR}/pmd-output.log"

find "." -name pmd-output.log -print0 \
  | xargs -0 sed -n -e '/^\[WARNING\] PMD Failure/p' \
  | tee "$REPORT_FILE"

## generate counter
grep -c ':' "$REPORT_FILE" > "$REPORT_DIR/failures"

ERROR_PATTERN="\[WARNING\] PMD Failure"
source "${DIR}/_post_process.sh"
