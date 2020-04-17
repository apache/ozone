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

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/integration"}
mkdir -p "$REPORT_DIR"

export MAVEN_OPTS="-Xmx4096m"
mvn -B install -DskipTests
mvn -B -fae test -pl :hadoop-ozone-integration-test,:mini-chaos-tests "$@" \
  | tee "${REPORT_DIR}/output.log"
rc=$?

# shellcheck source=hadoop-ozone/dev-support/checks/_mvn_unit_report.sh
source "$DIR/_mvn_unit_report.sh"

if [[ -s "$REPORT_DIR/summary.txt" ]] ; then
    exit 1
fi
exit ${rc}
