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

MAVEN_OPTIONS='-B -DskipTests -DskipDocs -DskipRecon -DskipShade --no-transfer-progress'

# Infer runs by wrapping javac during the Maven build to capture and analyze
# Java sources. We use 'compile' (not 'clean') to avoid deleting target/,
# which would wipe the REPORT_DIR. --keep-going tells Infer to continue past
# compilation errors in individual modules.
mkdir -p "$REPORT_DIR"
infer run --keep-going -- mvn ${MAVEN_OPTIONS} compile "$@" 2>&1 | tee "${REPORT_DIR}/output.log"
rc=$?

# Recreate REPORT_DIR after infer run, in case it was deleted (e.g., during
# a 'clean' phase or interleaved Maven runs).
mkdir -p "$REPORT_DIR"

# Copy infer output to report directory for artifact upload and reporting
if [[ -d infer-out ]]; then
  cp -r infer-out/* "${REPORT_DIR}/" 2>/dev/null || true
fi

# Infer's Maven integration may inject profiles into pom.xml files in
# profile-governed modules (ozonefs-hadoop2, ozonefs-shaded) and fail to
# restore them, leaving them corrupted. Restore from git checkout.
git checkout -- hadoop-ozone/ozonefs-hadoop2/pom.xml hadoop-ozone/ozonefs-shaded/pom.xml 2>/dev/null || true

touch "$REPORT_FILE"

if [[ -f "${REPORT_DIR}/report.txt" ]]; then
  echo "Infer analysis complete." >> "$REPORT_FILE"
  echo "" >> "$REPORT_FILE"
  cat "${REPORT_DIR}/report.txt" >> "$REPORT_FILE"

  # Count issue lines (lines containing .java: which indicate findings)
  grep -c '\.java:' "${REPORT_DIR}/report.txt" > "${REPORT_DIR}/failures" 2>/dev/null || echo "0" > "${REPORT_DIR}/failures"
else
  echo "Infer completed without generating issues." >> "$REPORT_FILE"
  echo "0" > "${REPORT_DIR}/failures"
fi

# Generate HTML summary if possible
if [[ -f "${REPORT_DIR}/report.txt" ]]; then
  infer report --issues-json "${REPORT_DIR}/report.json" 2>/dev/null || true
fi

ERROR_PATTERN="\[ERROR\]"
source "${DIR}/_post_process.sh"
