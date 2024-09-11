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

OZONE_ROOT=$(pwd -P)

: ${HADOOP_AWS_DIR:=""}
: ${OZONE_ACCEPTANCE_SUITE:=""}
: ${OZONE_TEST_SELECTOR:=""}
: ${OZONE_ACCEPTANCE_TEST_TYPE:="robot"}
: ${OZONE_WITH_COVERAGE:="false"}

source "${DIR}/_lib.sh"

REPORT_DIR=${OUTPUT_DIR:-"${OZONE_ROOT}/target/acceptance"}

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout -Dscan=false)
DIST_DIR="${OZONE_ROOT}/hadoop-ozone/dist/target/ozone-$OZONE_VERSION"

if [ ! -d "$DIST_DIR" ]; then
    echo "Distribution dir is missing. Doing a full build"
    "$DIR/build.sh" -Pcoverage
fi

mkdir -p "$REPORT_DIR"

if [[ "${OZONE_ACCEPTANCE_SUITE}" == "s3a" ]]; then
  OZONE_ACCEPTANCE_TEST_TYPE="maven"

  if [[ -z "${HADOOP_AWS_DIR}" ]]; then
    HADOOP_VERSION=$(mvn help:evaluate -Dexpression=hadoop.version -q -DforceStdout -Dscan=false)
    export HADOOP_AWS_DIR=${OZONE_ROOT}/target/hadoop-src
  fi

  download_hadoop_aws "${HADOOP_AWS_DIR}"
fi

export OZONE_ACCEPTANCE_SUITE OZONE_ACCEPTANCE_TEST_TYPE

cd "$DIST_DIR/compose" || exit 1
./test-all.sh 2>&1 | tee "${REPORT_DIR}/output.log"
RES=$?

if [[ "${OZONE_ACCEPTANCE_TEST_TYPE}" == "maven" ]]; then
  pushd result
  source "${DIR}/_mvn_unit_report.sh"
  find . -name junit -print0 | xargs -r -0 rm -frv
  cp -rv * "${REPORT_DIR}"/
  popd
else
  cp -rv result/* "$REPORT_DIR/"
  if [[ -f "${REPORT_DIR}/log.html" ]]; then
    cp "$REPORT_DIR/log.html" "$REPORT_DIR/summary.html"
  fi
  grep -A1 FAIL "${REPORT_DIR}/output.log" | grep -v '^Output' > "${REPORT_DIR}/summary.txt"
fi

find "$REPORT_DIR" -type f -empty -not -name summary.txt -print0 | xargs -0 rm -v

exit $RES
