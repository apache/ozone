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

set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

if [[ "$(uname -s)" = "Darwin" ]]; then
  echo "k3s is not supported on Mac" >&2
  exit 1
fi

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

_install_tool_callback() {
  # Install k3s
  curl -sfL https://get.k3s.io | sh -

  sudo chmod a+r $KUBECONFIG

  # Install flekszible
  wget https://github.com/elek/flekszible/releases/download/v1.8.1/flekszible_1.8.1_Linux_x86_64.tar.gz -O - | tar -zx
  chmod +x flekszible
  mkdir -p ${TOOL_DIR}/bin
  mv -iv flekszible ${TOOL_DIR}/bin/

  # Install robotframework
  sudo pip install robotframework
}

source "${DIR}/_lib.sh"
install_tool
export PATH="${PATH}:${TOOL_DIR}/bin"

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/kubernetes"}

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout)
DIST_DIR="$DIR/../../dist/target/ozone-$OZONE_VERSION"

if [ ! -d "$DIST_DIR" ]; then
    echo "Distribution dir is missing. Doing a full build"
    "$DIR/build.sh" -Pcoverage
fi

mkdir -p "$REPORT_DIR"

cd "$DIST_DIR/kubernetes/examples" || exit 1
./test-all.sh
RES=$?
cp -r result/* "$REPORT_DIR/"
cp "$REPORT_DIR/log.html" "$REPORT_DIR/summary.html"
exit $RES
