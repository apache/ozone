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
: ${OZONE_ACCEPTANCE_SUITE:="${1:-}"}
: ${OZONE_TEST_SELECTOR:=""}
: ${OZONE_ACCEPTANCE_TEST_TYPE:="robot"}
: ${OZONE_WITH_COVERAGE:="false"}

source "${DIR}/_lib.sh"

REPORT_DIR=${OUTPUT_DIR:-"${OZONE_ROOT}/target/acceptance"}
REPORT_FILE="$REPORT_DIR/summary.txt"

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout -Dscan=false)
DIST_DIR="${OZONE_ROOT}/hadoop-ozone/dist/target/ozone-$OZONE_VERSION"

# workaround attempt for https://github.com/docker/compose/issues/12747
export COMPOSE_PARALLEL_LIMIT=1

if [ ! -d "$DIST_DIR" ]; then
  echo "Error: distribution dir not found: $DIST_DIR"
  echo "Please build Ozone first."
  if [[ "${CI:-}" == "true" ]]; then
    ls -la "${OZONE_ROOT}/hadoop-ozone/dist/target"
  fi
  exit 1
fi

create_aws_dir

mkdir -p "$REPORT_DIR"

if [[ "${OZONE_ACCEPTANCE_SUITE}" == "s3a" ]]; then
  OZONE_ACCEPTANCE_TEST_TYPE="maven"

  if [[ -z "${HADOOP_AWS_DIR}" ]]; then
    hadoop_version=$(mvn help:evaluate -Dexpression=hadoop.version -q -DforceStdout -Dscan=false)
    export HADOOP_AWS_DIR=${OZONE_ROOT}/target/hadoop-src
  fi

  download_hadoop_aws() {
    local dir="$1"

    if [[ -z ${dir} ]]; then
      echo "Required argument: target directory for Hadoop AWS sources" >&2
      return 1
    fi

    if [[ ! -e "${dir}" ]] || [[ ! -d "${dir}"/src/test/resources ]]; then
      mkdir -p "${dir}"
      if [[ ! -f "${dir}.tar.gz" ]]; then
        local url="https://www.apache.org/dyn/closer.lua?action=download&filename=hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}-src.tar.gz"
        echo "Downloading Hadoop from ${url}"
        curl -LSs --fail -o "${dir}.tar.gz" "$url" || return 1
      fi
      tar -x -z -C "${dir}" --strip-components=3 -f "${dir}.tar.gz" --wildcards 'hadoop-*-src/hadoop-tools/hadoop-aws' || return 1
    fi
  }

  if ! download_hadoop_aws "${HADOOP_AWS_DIR}"; then
    echo "Failed to download Hadoop ${hadoop_version}" > "${REPORT_FILE}"
    exit 1
  fi
fi

export OZONE_ACCEPTANCE_SUITE OZONE_ACCEPTANCE_TEST_TYPE

cd "$DIST_DIR/compose" || exit 1
./test-all.sh 2>&1 | tee "${REPORT_DIR}/output.log"
rc=$?

if [[ "${OZONE_ACCEPTANCE_TEST_TYPE}" == "maven" ]]; then
  pushd result
  source "${DIR}/_mvn_unit_report.sh"
  find . -name junit -print0 | xargs -r -0 rm -frv
  cp -rv * "${REPORT_DIR}"/
  popd
  ERROR_PATTERN="\[ERROR\]"
else
  cp -rv result/* "$REPORT_DIR/"
  grep -A1 FAIL "${REPORT_DIR}/output.log" | grep -v '^Output' > "${REPORT_FILE}"
  ERROR_PATTERN="FAIL"
fi

find "$REPORT_DIR" -type f -empty -not -name summary.txt -print0 | xargs -0 rm -v

source "${DIR}/_post_process.sh"
