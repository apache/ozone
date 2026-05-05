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

set -u -o pipefail

#
# Test executor to test all the k8s/examples/*/test.sh test scripts.
#
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )

source "${SCRIPT_DIR}/testlib.sh"

ALL_RESULT_DIR="$SCRIPT_DIR/result"
rm "$ALL_RESULT_DIR"/* || true
mkdir -p "$ALL_RESULT_DIR"

RESULT=0
IFS=$'\n'
# shellcheck disable=SC2044
for test in $(find "$SCRIPT_DIR" -name test.sh | grep "${OZONE_TEST_SELECTOR:-""}" |sort); do
  TEST_DIR="$(dirname $test)"
  TEST_NAME="$(basename "$TEST_DIR")"

  echo ""
  echo "#### Executing tests of ${TEST_NAME} #####"
  echo ""
  cd "$TEST_DIR" || continue
  if ! ./test.sh; then
    RESULT=1
    echo "ERROR: Test execution of ${TEST_NAME} is FAILED!!!!"
  fi

  cp "$TEST_DIR"/result/output.xml "$ALL_RESULT_DIR"/"${TEST_NAME}".xml || true
  mkdir -p "$ALL_RESULT_DIR"/"${TEST_NAME}"
  mv "$TEST_DIR"/logs/*log "$ALL_RESULT_DIR"/"${TEST_NAME}"/ || true

  if [[ "${RESULT}" == "1" ]] && [[ "${FAIL_FAST:-}" == "true" ]]; then
    break
  fi
done

run_rebot "$ALL_RESULT_DIR" "$ALL_RESULT_DIR" "-N smoketests *.xml"

exit ${RESULT}
