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
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/infer"}
REPORT_FILE="$REPORT_DIR/summary.txt"
OUTPUT_LOG="${POM_BACKUP_DIR}/output.log"

restore_poms() {
  local rc=$?
  local restore_failed=false
  local i
  if ! mkdir -p "$REPORT_DIR" || ! cp "$OUTPUT_LOG" "${REPORT_DIR}/output.log"; then
    rc=1
  fi
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
  if [[ ${rc} -ne 0 ]]; then
    echo "Infer check failed with exit code ${rc}." >> "$REPORT_FILE"
    echo "1" > "${REPORT_DIR}/failures"
  fi
  exit "${rc}"
}

trap restore_poms EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$REPORT_DIR" || exit 1
rm -f "${REPORT_DIR}/report.txt" "${REPORT_DIR}/report.json" "${REPORT_DIR}/captured-sources.txt" \
  "${REPORT_DIR}/logs" infer-out/report.txt infer-out/report.json || exit 1
: > "$REPORT_FILE"
: > "$OUTPUT_LOG"
echo "0" > "${REPORT_DIR}/failures"
for i in "${!POM_FILES[@]}"; do
  cp "${POM_FILES[$i]}" "${POM_BACKUP_DIR}/${i}" || exit 1
done

source "${DIR}/_lib.sh"
source "${DIR}/install/infer.sh" >> "$OUTPUT_LOG" 2>&1

MAVEN_OPTIONS='-B -DskipTests -DskipDocs -DskipRecon -DskipShade -Dsort.skip=true --no-transfer-progress'
MAVEN_OPTIONS+=' -Ddevelocity.cache.local.enabled=false -Ddevelocity.cache.remote.enabled=false'

# Keep the log outside target because Maven clean removes it.
# Do not let Infer's Maven profile override each module's maven.compiler.release.
infer run --java-version-reset --keep-going --report-console-limit 0 \
  --skip-analysis-in-path '.*/src/test/.*' \
  --skip-analysis-in-path '.*/target/generated-test-sources/.*' \
  --skip-analysis-in-path '.*/target/generated-sources/.*' \
  --skip-analysis-in-path '.*/test-utils/.*' \
  -- mvn ${MAVEN_OPTIONS} clean install "$@" 2>&1 | tee -a "$OUTPUT_LOG"
infer_rc=$?

mkdir -p "$REPORT_DIR"

# Copy reports and diagnostics, not the multi-GB capture/results databases.
for report in report.txt report.json; do
  if [[ -f "infer-out/${report}" ]]; then
    cp "infer-out/${report}" "${REPORT_DIR}/" || exit 1
  fi
done
cp infer-out/logs "${REPORT_DIR}/" 2>/dev/null || true
cp -R infer-out/stats "${REPORT_DIR}/" 2>/dev/null || true

if [[ ${infer_rc} -ne 0 ]]; then
  echo "Infer exited with code ${infer_rc}." >> "$REPORT_FILE"
  exit 1
fi
if [[ ! -f "${REPORT_DIR}/report.txt" || ! -f "${REPORT_DIR}/report.json" ]]; then
  echo "Infer analysis produced no report." >> "$REPORT_FILE"
  exit 1
fi

if ! infer debug --source-files > "${REPORT_DIR}/captured-sources.txt" 2>> "$OUTPUT_LOG" \
    || ! grep -q '[^[:space:]]' "${REPORT_DIR}/captured-sources.txt"; then
  echo "Infer captured no source files or could not read the capture." >> "$REPORT_FILE"
  exit 1
fi

issue_count=$(jq -es '
  if length == 1 and (.[0] | type) == "array" then
    [.[0][] | select(.suppressed != true and .censored_reason == null)] | length
  else error("Expected a single JSON report array") end
' "${REPORT_DIR}/report.json" 2>> "$OUTPUT_LOG") || {
  echo "Infer produced an invalid JSON report." >> "$REPORT_FILE"
  exit 1
}
echo "Infer analysis complete. Found ${issue_count} issues." >> "$REPORT_FILE"
echo "${issue_count}" > "${REPORT_DIR}/failures"

# Findings are informational; execution and reporting failures are not.
exit 0
