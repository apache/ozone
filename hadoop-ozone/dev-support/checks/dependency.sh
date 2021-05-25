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

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/dependency"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"

hadoop-ozone/dist/src/main/license/update-jar-report.sh current.txt

cp hadoop-ozone/dist/src/main/license/jar-report.txt "$REPORT_DIR"
cp hadoop-ozone/dist/src/main/license/current.txt "$REPORT_DIR"

#implementation of sort cli is not exactly the same everywhere. It's better to sort with the same command locally
(diff -uw <(sort hadoop-ozone/dist/src/main/license/jar-report.txt) <(sort hadoop-ozone/dist/src/main/license/current.txt) || true ) > "$REPORT_FILE"


if [ -s "$REPORT_FILE" ]; then
  echo ""
   echo "Jar files are added/removed to/from the binary package."
   echo ""
   echo "Please update the hadoop-ozone/dist/src/main/license/bin/LICENSE.txt file with the modification"
   echo "   AND execute hadoop-ozone/dist/src/main/license/update-jar-report.sh when you are ready (after a full build)"
   echo ""
   echo "Generated hadoop-ozone/dist/src/main/license/jar-report.txt file should be added to your pull-request. It will be used as the base of future comparison."
   echo ""
   echo "This check may also report positive for PRs if the source is not up-to-date with the base branch (eg. \`master\`).  In this case please merge the base branch into your source branch."
   echo ""
   echo "Changed jars:"
   echo ""
   cat $REPORT_FILE
   exit 1
fi
