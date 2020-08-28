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


#
# Test executor to test all the compose/*/test.sh test scripts.
#
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )

set -ex

ALL_RESULT_DIR="$SCRIPT_DIR/result"
rm "$ALL_RESULT_DIR/*" || true
mkdir -p "$ALL_RESULT_DIR"

RESULT=0
IFS=$'\n'
# shellcheck disable=SC2044
for test in $(find "$SCRIPT_DIR" -name test.sh | grep "${OZONE_TEST_SELECTOR:-""}" |sort); do
  TEST_DIR="$(dirname $test)"
  TEST_NAME="$(basename "$TEST_DIR")"

  echo ""
  echo "#### Executing tests of ${TEST_DIR} #####"
  echo ""
  cd "$TEST_DIR" || continue
  ./test.sh

  cp "$TEST_DIR"/result/output.xml "$ALL_RESULT_DIR"/"${TEST_NAME}".xml
  mkdir -p "$ALL_RESULT_DIR"/"${TEST_NAME}"
  mv "$TEST_DIR"/logs/*log "$ALL_RESULT_DIR"/"${TEST_NAME}"/
done

rebot -N "smoketests" -d "$ALL_RESULT_DIR/" "$ALL_RESULT_DIR/*.xml"

