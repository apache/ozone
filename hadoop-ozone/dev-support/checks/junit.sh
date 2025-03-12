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

: ${CHECK:="unit"}
: ${FAIL_FAST:="false"}
: ${ITERATIONS:="1"}
: ${OZONE_WITH_COVERAGE:="false"}
: ${OZONE_REPO_CACHED:="false"}
: ${TARGET_TEST_CLASS:=""}

declare -i ITERATIONS
if [[ ${ITERATIONS} -le 0 ]]; then
  ITERATIONS=1
fi

export MAVEN_OPTS="-Xmx4096m ${MAVEN_OPTS:-}"
MAVEN_OPTIONS="-B -V -DskipRecon -Dnative.lib.tmp.dir=/tmp --no-transfer-progress"

if [[ "${OZONE_WITH_COVERAGE}" != "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} -Djacoco.skip"
fi

if [[ "${FAIL_FAST}" == "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-fast -Dsurefire.skipAfterFailureCount=1"
else
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-at-end"
fi

# apply module access args (for Java 9+)
OZONE_MODULE_ACCESS_ARGS=""
if [[ -f hadoop-ozone/dist/src/shell/ozone/ozone-functions.sh ]]; then
  source hadoop-ozone/dist/src/shell/ozone/ozone-functions.sh
  ozone_java_setup
fi

if [[ ${ITERATIONS} -gt 1 ]] && [[ ${OZONE_REPO_CACHED} == "false" ]]; then
  mvn ${MAVEN_OPTIONS} -DskipTests clean install
fi

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/${CHECK}"}
REPORT_FILE="${REPORT_DIR}/summary.txt"
mkdir -p "$REPORT_DIR"

echo "TARGET_TEST_CLASS: ${TARGET_TEST_CLASS}"

# Find the project containing the test class if specified
PROJECT_LIST=""
if [[ -n "${TARGET_TEST_CLASS}" ]] && [[ "${TARGET_TEST_CLASS}" != "Abstract"* ]]; then
  echo "Finding project for test class: ${TARGET_TEST_CLASS}"

  # Extract the base part of the class name (before any wildcard)
  BASE_CLASS_NAME="${TARGET_TEST_CLASS%%\**}"
  BASE_CLASS_NAME="${BASE_CLASS_NAME%%\?*}"

  # If the base name is empty after removing wildcards, use a reasonable default
  if [[ -z "${BASE_CLASS_NAME}" ]]; then
    echo "Test class name contains only wildcards, searching in all test directories"
    # Set PROJECT_LIST to empty to run in all modules
  else
    echo "Searching for files matching base name: ${BASE_CLASS_NAME}"

    # Find all projects containing matching test classes - use a more flexible search approach
    # First try direct filename search
    TEST_FILES=($(find . -path "*/src/test/*" -name "${BASE_CLASS_NAME}*.java" | sort -u))

    # If no files found and the class name contains dots (package notation), try searching by path
    if [[ ${#TEST_FILES[@]} -eq 0 && "${BASE_CLASS_NAME}" == *"."* ]]; then
      # Convert base class to path format
      TEST_CLASS_PATH="${BASE_CLASS_NAME//./\/}.java"
      echo "No files found with direct name search, trying path-based search"
      echo "TEST_CLASS_PATH pattern: ${TEST_CLASS_PATH}"

      # Search by path pattern
      TEST_FILES=($(find . -path "*/src/test/*/${TEST_CLASS_PATH%.*}*.java" | sort -u))
    fi

    echo "Found ${#TEST_FILES[@]} matching test file(s)"

    if [[ ${#TEST_FILES[@]} -gt 0 ]]; then
      # Extract project paths (up to the src/test directory)
      PROJECT_PATHS=()
      for TEST_FILE in "${TEST_FILES[@]}"; do
        echo "TEST_FILE: ${TEST_FILE}"
        PROJECT_PATH=$(dirname "${TEST_FILE}" | sed -e 's|/src/test.*||')
        if [[ -f "${PROJECT_PATH}/pom.xml" ]]; then
          echo "Found test in project: ${PROJECT_PATH}"
          PROJECT_PATHS+=("${PROJECT_PATH}")
        fi
      done

      # Join project paths with commas for Maven -pl option
      if [[ ${#PROJECT_PATHS[@]} -gt 0 ]]; then
        IFS=","
        PROJECT_LIST="-pl ${PROJECT_PATHS[*]}"
        unset IFS
        echo "Setting project list to: ${PROJECT_LIST}"
      fi
    else
      echo "Could not find project for test class pattern: ${TARGET_TEST_CLASS}"
    fi
  fi
fi

rc=0
for i in $(seq 1 ${ITERATIONS}); do
  if [[ ${ITERATIONS} -gt 1 ]]; then
    original_report_dir="${REPORT_DIR}"
    REPORT_DIR="${original_report_dir}/iteration${i}"
    mkdir -p "${REPORT_DIR}"
  fi

  mvn ${MAVEN_OPTIONS} ${PROJECT_LIST} -Dmaven-surefire-plugin.argLineAccessArgs="${OZONE_MODULE_ACCESS_ARGS}" "$@" verify \
    | tee "${REPORT_DIR}/output.log"
  irc=$?

  # shellcheck source=hadoop-ozone/dev-support/checks/_mvn_unit_report.sh
  source "${DIR}/_mvn_unit_report.sh"
  if [[ ${irc} == 0 ]] && [[ -s "${REPORT_DIR}/summary.txt" ]]; then
    irc=1
  fi

  if [[ ${ITERATIONS} -gt 1 ]]; then
    if ! grep -q "Running .*Test" "${REPORT_DIR}/output.log"; then
      echo "No tests were run" >> "${REPORT_DIR}/summary.txt"
      irc=1
      FAIL_FAST=true
    fi

    if [[ ${irc} == 0 ]]; then
      rm -fr "${REPORT_DIR}"
    fi

    REPORT_DIR="${original_report_dir}"
    echo "Iteration ${i} exit code: ${irc}" | tee -a "${REPORT_FILE}"
  fi

  if [[ ${rc} == 0 ]]; then
    rc=${irc}
  fi

  if [[ ${rc} != 0 ]] && [[ "${FAIL_FAST}" == "true" ]]; then
    break
  fi
done

if [[ "${OZONE_WITH_COVERAGE}" == "true" ]]; then
  #Archive combined jacoco records
  mvn -B -N jacoco:merge -Djacoco.destFile=$REPORT_DIR/jacoco-combined.exec -Dscan=false
fi

ERROR_PATTERN="\[ERROR\]"
source "${DIR}/_post_process.sh"
