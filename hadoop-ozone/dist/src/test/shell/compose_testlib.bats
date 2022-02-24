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


load ../../main/compose/testlib.sh
@test "Find test recursive, only on one level" {
  cd $BATS_TEST_DIRNAME
  result=$(all_tests_in_immediate_child_dirs | xargs)
  [[ "$result" == "failing1/test.sh test1/test.sh test2/test.sh test4/test.sh" ]]
}

@test "Find tests without explicit filter" {
  cd $BATS_TEST_DIRNAME
  run find_tests
  [[ "$output" == "test1/test.sh test2/test.sh test4/test.sh" ]]
}

@test "Find test by suite" {
  OZONE_ACCEPTANCE_SUITE=one
  cd $BATS_TEST_DIRNAME
  run find_tests
  [[ "$output" == "test4/test.sh" ]]
}

@test "Find failing test suite explicitly" {
  OZONE_ACCEPTANCE_SUITE=failing
  cd $BATS_TEST_DIRNAME
  run find_tests
  [[ "$output" == "failing1/test.sh" ]]
}

@test "Find test default suite" {
  OZONE_ACCEPTANCE_SUITE=misc
  cd $BATS_TEST_DIRNAME
  run find_tests
  [[ "$output" == "test1/test.sh test2/test.sh" ]]
}
