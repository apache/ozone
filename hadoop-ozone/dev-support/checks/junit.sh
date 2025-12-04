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

# Handle cancellation signals
cancelled=false
trap 'cancelled=true; echo "Caught cancellation signal, exiting..."; exit 130' SIGINT SIGTERM

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

: ${CHECK:="unit"}
: ${FAIL_FAST:="false"}
: ${ITERATIONS:="1"}
: ${OZONE_WITH_COVERAGE:="false"}
: ${OZONE_REPO_CACHED:="false"}

declare -i ITERATIONS
if [[ ${ITERATIONS} -le 0 ]]; then
  ITERATIONS=1
fi

export MAVEN_OPTS="-Xmx4096m ${MAVEN_OPTS:-}"
MAVEN_OPTIONS="-B -V -DskipDocs -DskipRecon -Dnative.lib.tmp.dir=/tmp --no-transfer-progress"

if [[ "${OZONE_WITH_COVERAGE}" != "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} -Djacoco.skip"
fi

if [[ "${FAIL_FAST}" == "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-fast -Dsurefire.skipAfterFailureCount=1"
else
  MAVEN_OPTIONS="${MAVEN_OPTIONS} --fail-never"
fi

# apply module access args (for Java 9+)
OZONE_MODULE_ACCESS_ARGS=""
if [[ -f hadoop-ozone/dist/src/shell/ozone/ozone-functions.sh ]]; then
  source hadoop-ozone/dist/src/shell/ozone/ozone-functions.sh
  ozone_java_setup
fi

mvn ${MAVEN_OPTIONS} clean

if [[ ${ITERATIONS} -gt 1 ]] && [[ ${OZONE_REPO_CACHED} == "false" ]]; then
  mvn ${MAVEN_OPTIONS} -DskipTests install
fi

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/${CHECK}"}
REPORT_FILE="${REPORT_DIR}/summary.txt"
mkdir -p "$REPORT_DIR"

rc=0
for i in $(seq 1 ${ITERATIONS}); do
  if [[ "${cancelled}" == "true" ]]; then
    echo "Cancellation detected, stopping test iterations"
    break
  fi

  if [[ ${ITERATIONS} -gt 1 ]]; then
    original_report_dir="${REPORT_DIR}"
    REPORT_DIR="${original_report_dir}/iteration${i}"
    mkdir -p "${REPORT_DIR}"
  fi

  mvn ${MAVEN_OPTIONS} -Dmaven-surefire-plugin.argLineAccessArgs="${OZONE_MODULE_ACCESS_ARGS}" "$@" verify \
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

source "${DIR}/_post_process.sh"
