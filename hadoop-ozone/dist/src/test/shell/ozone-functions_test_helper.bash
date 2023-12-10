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

setup() {
  RELTMP="${BATS_TEST_DIRNAME}/../../../target/test-dir/bats.$$.${RANDOM}"
  mkdir -p "${RELTMP}"

  TMP=$(cd -P -- "${RELTMP}" >/dev/null && pwd -P)
  export TMP

  # shellcheck disable=SC2034
  OZONE_SHELL_SCRIPT_DEBUG=true

  # shellcheck disable=SC2034
  QATESTMODE=true

  . "${BATS_TEST_DIRNAME}/../../shell/ozone/ozone-functions.sh"
  pushd "${TMP}" >/dev/null
}

teardown() {
  popd >/dev/null
  rm -rf "${TMP}"
}
