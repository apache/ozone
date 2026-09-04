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
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/errorprone"}
REPORT_FILE="$REPORT_DIR/summary.txt"
DIAGNOSTIC_FILE="$REPORT_DIR/diagnostics.txt"
OUTPUT_LOG=$(mktemp)

MAVEN_OPTIONS='-B -fae --no-transfer-progress -Perrorprone -DskipDocs -DskipRecon -DskipShade'
MAVEN_DIAGNOSTIC_PATTERN='^\[(ERROR|WARNING)\] .*:\[[0-9]+,[0-9]+\] \[[^]]+\]'
JAVAC_DIAGNOSTIC_PATTERN='^.*:[0-9]+: (error|warning): \[[^]]+\]'
MAVEN_ERROR_PATTERN='^\[ERROR\] .*:\[[0-9]+,[0-9]+\] \[[^]]+\]'
JAVAC_ERROR_PATTERN='^.*:[0-9]+: error: \[[^]]+\]'
ERROR_DIAGNOSTIC_PATTERN="${MAVEN_ERROR_PATTERN}|${JAVAC_ERROR_PATTERN}"

declare -i rc

trap 'rm -f "$OUTPUT_LOG"' EXIT

#shellcheck disable=SC2086
mvn $MAVEN_OPTIONS clean test-compile "$@" 2>&1 | tee "$OUTPUT_LOG"
rc=$?

mkdir -p "$REPORT_DIR"
mv "$OUTPUT_LOG" "${REPORT_DIR}/output.log"
trap - EXIT

grep -E "${MAVEN_DIAGNOSTIC_PATTERN}|${JAVAC_DIAGNOSTIC_PATTERN}" "${REPORT_DIR}/output.log" \
  | awk '!seen[$0]++' > "$DIAGNOSTIC_FILE"

grep -E "$ERROR_DIAGNOSTIC_PATTERN" "$DIAGNOSTIC_FILE" > "$REPORT_FILE" || true

source "${DIR}/_post_process.sh"
