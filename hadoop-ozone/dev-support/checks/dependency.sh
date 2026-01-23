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

src_dir=hadoop-ozone/dist/src/main/license

${src_dir}/update-jar-report.sh

cp ${src_dir}/jar-report.txt "$REPORT_DIR"/
cp ${src_dir}/current.txt "$REPORT_DIR"/

#implementation of sort cli is not exactly the same everywhere. It's better to sort with the same command locally
(diff -uw \
  <(sort -u ${src_dir}/jar-report.txt) \
  <(sort -u ${src_dir}/current.txt) \
  || true) \
  > "$REPORT_FILE"

if [ -s "$REPORT_FILE" ]; then
  cat <<-EOF
Jar files under share/ozone/lib in the build have changed.

Please update:

 * ${src_dir}/bin/LICENSE.txt
   (add new dependencies with the appropriate license, delete any removed dependencies)

 * ${src_dir}/jar-report.txt
   (based on the diff shown below)

If you notice unexpected differences (can happen when the check is run in a
fork), please first update your branch from upstream master to get any other
recent dependency changes.

If you are running this locally after build with -DskipShade, please ignore any
ozone-filesystem jars reported to be missing.

Changes detected:

EOF

  cat "$REPORT_FILE"
  exit 1
fi
