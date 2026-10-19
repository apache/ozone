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

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

POM_FILES=(hadoop-ozone/ozonefs-hadoop2/pom.xml hadoop-ozone/ozonefs-shaded/pom.xml)
if ! git diff --quiet -- "${POM_FILES[@]}" || ! git diff --cached --quiet -- "${POM_FILES[@]}"; then
  echo "Infer requires clean ozonefs POMs. Commit or stash changes to these files first:" >&2
  printf '  %s\n' "${POM_FILES[@]}" >&2
  exit 1
fi

POM_BACKUP_DIR=$(mktemp -d) || exit 1

restore_poms() {
  local rc=$?
  local restore_failed=false
  local i
  for i in "${!POM_FILES[@]}"; do
    if [[ -f "${POM_BACKUP_DIR}/${i}" ]] && ! cp "${POM_BACKUP_DIR}/${i}" "${POM_FILES[$i]}"; then
      restore_failed=true
    fi
  done
  if [[ "${restore_failed}" == "true" ]]; then
    echo "Failed to restore ozonefs POMs. Original files are saved in ${POM_BACKUP_DIR}" >&2
    rc=1
  else
    rm -rf "${POM_BACKUP_DIR}"
  fi
  exit "${rc}"
}

trap restore_poms EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for i in "${!POM_FILES[@]}"; do
  cp "${POM_FILES[$i]}" "${POM_BACKUP_DIR}/${i}" || exit 1
done

source "${DIR}/_lib.sh"
source "${DIR}/install/infer.sh"

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/infer"}
REPORT_FILE="$REPORT_DIR/summary.txt"

MAVEN_OPTIONS='-B -DskipTests -DskipDocs -DskipRecon -DskipShade -Dsort.skip=true --no-transfer-progress'

mkdir -p "$REPORT_DIR"
infer run --keep-going \
  --skip-analysis-in-path '.*/src/test/.*' \
  --skip-analysis-in-path '.*/target/generated-test-sources/.*' \
  --skip-analysis-in-path '.*/target/generated-sources/.*' \
  --skip-analysis-in-path '.*/test-utils/.*' \
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
