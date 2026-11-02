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
CHECK=integration
ERROR_PATTERN="\[ERROR\]"

PHASE=test

args=""

if [[ "$@" =~ -Ptest-([a-z]+) ]]; then
  # test-... profile
  split="${BASH_REMATCH[1]}"
  args="$args -Dscan.value.testset=${split}"

  if [[ "${split}" == "flaky" ]]; then
    args="$args -Dsurefire.rerunFailingTestsCount=5 -Dsurefire.fork.timeout=3600"
    # tests may pass on re-run, so we cannot rely on output for status
    ERROR_PATTERN=""
    # flaky tests may be present in any module, not setting project list
    # but skip shaded ones
    args="$args -DskipShade"
  else
    # apply profile only to the integration test module
    # ozone-main is needed for fetching libhadoop
    args="$args -pl :ozone-main,:ozone-integration-test"

    if [[ "${CI:-}" != "true" ]]; then
      # build requirements, too
      args="$args --also-make"
    fi
  fi

elif [[ "$@" =~ "-pl :([a-z0-9-]+)" ]]; then
  # specific module
  split="${BASH_REMATCH[1]}"
  args="$args -Dscan.value.testset=${split}"

  if [[ "${split}" =~ "ozone-filesystem-hadoop.*" ]] && [[ "${CI:-}" != "true" ]]; then
    # shading is performed in `package` phase, which is after `test`
    PHASE=verify
  fi

  # --also-make is not added even for local run, since without test filter all tests would be run
  # for local test, please build before running this script
fi

source "${DIR}/junit.sh" $args "$@"
