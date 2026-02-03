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
REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/checkstyle"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"
UI_REPORT_FILE="$REPORT_DIR/ui-summary.txt"

MAVEN_OPTIONS='-B -fae -DskipDocs -DskipRecon -Dcheckstyle.failOnViolation=false --no-transfer-progress'

MODE="${1:-unknown}"

declare -i rc
declare -i java_rc
declare -i ui_rc

run_java_checkstyle() {
  mvn ${MAVEN_OPTIONS} checkstyle:check > "${REPORT_DIR}/output.log"
  java_rc=$?
  if [[ ${java_rc} -ne 0 ]]; then
    mvn ${MAVEN_OPTIONS} clean test-compile checkstyle:check > output.log
    java_rc=$?
    mkdir -p "$REPORT_DIR" # removed by mvn clean
    mv output.log "${REPORT_DIR}"/
  fi

  cat "${REPORT_DIR}/output.log"

  # Print out the exact violations by parsing XML results with sed
  find "." -name checkstyle-errors.xml -print0 \
    | xargs -0 sed '$!N; /<file.*\n<\/file/d;P;D' \
    | sed \
        -e '/<?xml.*>/d' \
        -e '/<checkstyle.*/d' \
        -e '/<\/.*/d' \
        -e 's/<file name="\([^"]*\)".*/\1/' \
        -e 's/<error.*line="\([[:digit:]]*\)".*message="\([^"]*\)".*/ \1: \2/' \
        -e "s!^${BASE_DIR}/!!" \
        -e "s/&apos;/'/g" \
        -e "s/&lt;/</g" \
        -e "s/&gt;/>/g" \
    | tee "$REPORT_FILE"
}

run_ui_lint() {
  if [[ -d "${BASE_DIR}/ozone-ui/src" ]]; then
    (
      cd "${BASE_DIR}/ozone-ui/src" || exit 1
      pnpm -s run lint -- --format unix
    ) > "${REPORT_DIR}/ui-output.log" 2>&1
    ui_rc=$?
    if [[ ${ui_rc} -ne 0 ]]; then
      cat "${REPORT_DIR}/ui-output.log" > "$UI_REPORT_FILE"
      if [[ ! -s "$UI_REPORT_FILE" ]]; then
        echo "UI lint failed. See ${REPORT_DIR}/ui-output.log for details." > "$UI_REPORT_FILE"
      fi
    fi
  else
    ui_rc=0
    echo "ozone-ui/src not found. Skipping UI lint." > "$UI_REPORT_FILE"
  fi
}

case "${MODE}" in
  java)
    run_java_checkstyle
    rc=${java_rc}

    ## generate counter
    grep -c ':' "$REPORT_FILE" > "$REPORT_DIR/failures"

    ERROR_PATTERN="\[ERROR\]"
    source "${DIR}/_post_process.sh"
    ;;
  ui)
    run_ui_lint
    if [[ -s "$UI_REPORT_FILE" ]]; then
      cat "$UI_REPORT_FILE" > "$REPORT_FILE"
    fi

    if [[ ! -s "${REPORT_DIR}/failures" ]]; then
      wc -l "$REPORT_FILE" | awk '{ print $1 }' > "$REPORT_DIR/failures"
    fi

    rc=${ui_rc}
    if [[ -s "$REPORT_FILE" ]]; then
      rc=1
    fi
    exit ${rc}
    ;;
  *)
    echo "Unknown mode: ${MODE}. Expected java or ui."
    exit 2
    ;;
esac
