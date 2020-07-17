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
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
ALL_RESULT_DIR="$SCRIPT_DIR/result"

RESULT=0
IFS=$'\n'
# shellcheck disable=SC2044
for test in $(find "$SCRIPT_DIR" -mindepth 2 -maxdepth 2 -name test.sh | grep "${OZONE_TEST_SELECTOR:-""}" |sort); do
  TEST_DIR="$(dirname "$test")"
  echo "Executing test in $TEST_DIR"

  #required to read the .env file from the right location
  cd "$TEST_DIR" || continue
  ./test.sh || echo "ERROR: Test execution of $TEST_DIR is FAILED!!!!"
  RESULT_DIR="$TEST_DIR"/result
  rebot -N $(basename $TEST_DIR) -o "$ALL_RESULT_DIR"/$(basename $TEST_DIR).xml "$RESULT_DIR"/*.xml
  cp "$RESULT_DIR"/output.xml "$RESULT_DIR"/docker-*.log "$RESULT_DIR"/*.out* "$ALL_RESULT_DIR"/
done
