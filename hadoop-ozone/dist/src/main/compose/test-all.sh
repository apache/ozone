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
ALL_RESULT_DIR="$SCRIPT_DIR/result"
PROJECT_DIR="$SCRIPT_DIR/.."
mkdir -p "$ALL_RESULT_DIR"
rm "$ALL_RESULT_DIR/*" || true

source "$SCRIPT_DIR"/testlib.sh

if [ "$OZONE_WITH_COVERAGE" ]; then
   java -cp "$PROJECT_DIR"/share/coverage/$(ls "$PROJECT_DIR"/share/coverage | grep test-util):"$PROJECT_DIR"/share/coverage/jacoco-core.jar org.apache.hadoop.test.JacocoServer &
   DOCKER_BRIDGE_IP=$(docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}')
   export OZONE_OPTS="-javaagent:share/coverage/jacoco-agent.jar=output=tcpclient,address=$DOCKER_BRIDGE_IP,includes=org.apache.hadoop.ozone.*:org.apache.hadoop.hdds.*:org.apache.hadoop.fs.ozone.*"
fi

tests=$(find_tests)
cd "$SCRIPT_DIR"

RESULT=0
# shellcheck disable=SC2044
for t in ${tests}; do
  d="$(dirname "${t}")"

  if ! run_test_script "${d}"; then
    RESULT=1
  fi

  copy_results "${d}" "${ALL_RESULT_DIR}"
done

rebot --nostatusrc -N acceptance -d "$ALL_RESULT_DIR" "$ALL_RESULT_DIR"/*.xml

if [ "$OZONE_WITH_COVERAGE" ]; then
  pkill -f JacocoServer
  cp /tmp/jacoco-combined.exec "$SCRIPT_DIR"/result
fi

exit $RESULT
