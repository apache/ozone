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

export KUBECONFIG

REPORT_DIR=${OUTPUT_DIR:-"${OZONE_ROOT}/target/kubernetes"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"

source "${DIR}/_lib.sh"
source "${DIR}/install/flekszible.sh"

if [[ "$(uname -s)" = "Darwin" ]]; then
  echo "Skip installing k3s, not supported on Mac.  Make sure a working Kubernetes cluster is available." >&2
else
  source "${DIR}/install/k3s.sh"
fi

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout -Dscan=false)
DIST_DIR="${OZONE_ROOT}/hadoop-ozone/dist/target/ozone-$OZONE_VERSION"

if [ ! -d "$DIST_DIR" ]; then
  echo "Error: distribution dir not found: $DIST_DIR"
  echo "Please build Ozone first."
  if [[ "${CI:-}" == "true" ]]; then
    ls -la "${OZONE_ROOT}/hadoop-ozone/dist/target"
  fi
  exit 1
fi

create_aws_dir

cd "$DIST_DIR/kubernetes/examples" || exit 1
./test-all.sh 2>&1 | tee "${REPORT_DIR}/output.log"
rc=$?
cp -r result/* "$REPORT_DIR/"

grep -A1 FAIL "${REPORT_DIR}/output.log" > "${REPORT_FILE}"

ERROR_PATTERN="FAIL"
source "${DIR}/_post_process.sh"
