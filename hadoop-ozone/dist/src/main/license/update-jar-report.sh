#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
REPORT_NAME=${1:-jar-report.txt}

cd "$SCRIPTDIR/../../.." || exit 1

OZONE_VERSION=$(mvn help:evaluate -Dexpression=ozone.version -q -DforceStdout)
DIST_DIR="$SCRIPTDIR/../../../target/ozone-$OZONE_VERSION"
: ${OZONE_DIST_DIR:=$DIST_DIR}

cd "$OZONE_DIST_DIR"

#sed expression removes the version. Usually license is not changed with version bumps
#jacoco and test dependencies are excluded
find . -type f -name "*.jar" | cut -c3- | perl -wpl -e 's/-[0-9]+(.[0-9]+)*(-([0-9a-z]+-)?SNAPSHOT)?+//g' | grep -v -e jacoco -e hdds-test-utils | sort > "$SCRIPTDIR"/$REPORT_NAME
