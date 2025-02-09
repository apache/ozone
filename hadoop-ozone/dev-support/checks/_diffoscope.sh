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

# Helper script to compare jars reported by maven-artifact-plugin

set -e -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

BASE_DIR="$(pwd -P)"
: ${OUTPUT_LOG:="${BASE_DIR}/target/repro/output.log"}

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  sudo apt update -q
  sudo apt install -y diffoscope
fi

for jar in $(grep -o "investigate with diffoscope [^ ]*\.jar [^ ]*\.jar" "${OUTPUT_LOG}" | awk '{ print $NF }'); do
  jarname=$(basename "$jar")
  if [[ ! -e "$jar" ]]; then
    echo "$jar does not exist"
    continue
  fi

  ref=$(find target/reference -name "$jarname")
  if [[ -z "$ref" ]]; then
    ref=$(find ~/.m2/repository -name "$jarname")
  fi

  if [[ ! -e "$ref" ]]; then
    echo "Reference not found for: $jarname"
    continue
  fi

  diffoscope "$ref" "$jar"
done
