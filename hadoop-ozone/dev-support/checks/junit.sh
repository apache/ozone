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
: ${ITERATIONS:="1"}

export MAVEN_OPTS="-Xmx4096m"
MAVEN_OPTIONS='-B -Dskip.npx -Dskip.installnpx'
mvn ${MAVEN_OPTIONS} -DskipTests clean install

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/${CHECK}"}
mkdir -p "$REPORT_DIR"

rc=0
for i in $(seq 1 ${ITERATIONS}); do
  if [[ ${ITERATIONS} -gt 1 ]]; then
    original_report_dir="${REPORT_DIR}"
    REPORT_DIR="${original_report_dir}/iteration${i}"
    mkdir -p "${REPORT_DIR}"
  fi

  mvn ${MAVEN_OPTIONS} -fae "$@" test \
    | tee "${REPORT_DIR}/output.log"
  irc=$?

  # shellcheck source=hadoop-ozone/dev-support/checks/_mvn_unit_report.sh
  source "${DIR}/_mvn_unit_report.sh"
  if [[ ${irc} == 0 ]] && [[ -s "${REPORT_DIR}/summary.txt" ]]; then
    irc=1
  fi

  if [[ ${ITERATIONS} -gt 1 ]]; then
    REPORT_DIR="${original_report_dir}"
    echo "Iteration ${i} exit code: ${irc}" | tee -a "${REPORT_DIR}/summary.txt"
  fi

  if [[ ${rc} == 0 ]]; then
    rc=${irc}
  fi
done

#Archive combined jacoco records
mvn -B -N jacoco:merge -Djacoco.destFile=$REPORT_DIR/jacoco-combined.exec

exit ${rc}
