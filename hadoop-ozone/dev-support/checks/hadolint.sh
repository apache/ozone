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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1
REPO_DIR="$DIR/../../.."

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/hadolint"}
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/summary.txt"
echo -n > "$REPORT_FILE"

for Dockerfile in $(find hadoop-ozone hadoop-hdds -name Dockerfile | sort); do
  hadolint $Dockerfile | tee -a "$REPORT_FILE"
done

wc -l "$REPORT_FILE" | awk '{print $1}'> "$REPORT_DIR/failures"

if [ -s "${REPORT_FILE}" ]
then
  echo "" | tee -a "$REPORT_FILE"
  echo "" | tee -a "$REPORT_FILE"
  echo "Hadolint errors were found. Exit code: 1." | tee -a "$REPORT_FILE"
  exit 1
fi
