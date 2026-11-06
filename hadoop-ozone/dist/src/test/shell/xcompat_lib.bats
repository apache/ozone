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

# Usage:
#     hadoop-ozone/dev-support/checks/bats.sh hadoop-ozone/dist/src/test/shell/xcompat_lib.bats

: ${TOOLS_DIR:="${BATS_ROOT}/../.."}
load ${TOOLS_DIR}/bats/bats-support/load.bash
load ${TOOLS_DIR}/bats/bats-assert/load.bash

load ../../main/compose/xcompat/lib.sh

@test "Cross-version reads in old cluster" {
  old_versions="1.2.3 4.5.6"
  current_version="7.8.9"

  for cluster_version in $old_versions; do
    run get_read_version_combinations
    assert_output # is not empty

    # old clients read new data
    assert_line '1.2.3=7.8.9'
    assert_line '4.5.6=7.8.9'
    # new client reads all data
    assert_line '7.8.9=1.2.3'
    assert_line '7.8.9=4.5.6'
    assert_line '7.8.9=7.8.9'

    assert_equal "$(wc -l <<< "$output")" '5'
  done
}

@test "Cross-version reads in new cluster" {
  old_versions="1.2.3 4.5.6"
  current_version="7.8.9"
  cluster_version="${current_version}"

  run get_read_version_combinations
  assert_output # is not empty

  all="${old_versions} ${current_version}"

  # each client reads all data
  for r in ${all}; do
    for w in ${all}; do
      assert_line "$r=$w"
    done
  done

  assert_equal "$(wc -l <<< "$output")" '9'
}
