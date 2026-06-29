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

source "${DIR}/_lib.sh"
source "${DIR}/install/infer.sh"

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/infer"}
REPORT_FILE="$REPORT_DIR/summary.txt"

MAVEN_OPTIONS='-B -DskipTests -DskipDocs -DskipRecon -DskipShade -Dsort.skip=true --no-transfer-progress'

# Restore pom.xml files that may have been corrupted by a previous infer run
# (infer injects profiles into pom.xml and sometimes fails to restore them).
git checkout -- hadoop-ozone/ozonefs-hadoop2/pom.xml hadoop-ozone/ozonefs-shaded/pom.xml 2>/dev/null || true

mkdir -p "$REPORT_DIR"
infer run --keep-going \
  --skip-analysis-in-path "src/test/" \
  --skip-analysis-in-path "target/generated-test-sources/" \
  --skip-analysis-in-path "target/generated-sources/" \
  -- mvn ${MAVEN_OPTIONS} install "$@" 2>&1 | tee "${REPORT_DIR}/output.log"
infer_rc=$?

mkdir -p "$REPORT_DIR"

# Only copy text reports, not the multi-GB capture/results databases
if [[ -f infer-out/report.txt ]]; then
  cp infer-out/report.txt "${REPORT_DIR}/"
  cp infer-out/logs "${REPORT_DIR}/" 2>/dev/null || true
  cp infer-out/stats "${REPORT_DIR}/" -r 2>/dev/null || true
elif [[ -d infer-out ]]; then
  find infer-out -name "report.txt" -exec cp {} "${REPORT_DIR}/" \; 2>/dev/null || true
fi

# Restore pom.xml files again after infer (belt and suspenders)
git checkout -- hadoop-ozone/ozonefs-hadoop2/pom.xml hadoop-ozone/ozonefs-shaded/pom.xml 2>/dev/null || true

touch "$REPORT_FILE"

if [[ -f "${REPORT_DIR}/report.txt" ]]; then
  issue_count=$(grep -c '\.java:' "${REPORT_DIR}/report.txt" 2>/dev/null || echo "0")
  echo "Infer analysis complete. Found ${issue_count} issues." >> "$REPORT_FILE"
  echo "${issue_count}" > "${REPORT_DIR}/failures"
else
  echo "Infer analysis produced no report." >> "$REPORT_FILE"
  echo "0" > "${REPORT_DIR}/failures"
fi

# Infer findings are informational, never a failure.
# Only exit non-zero if infer itself failed without producing any report.
if [[ -f "${REPORT_DIR}/report.txt" ]]; then
  exit 0
elif [[ ${infer_rc} -ne 0 ]]; then
  echo "Infer exited with code ${infer_rc} and no report was produced." >> "$REPORT_FILE"
  exit 1
else
  exit 0
fi
