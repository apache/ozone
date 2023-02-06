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

# This script confirms that pr_title_check.sh works correctly
# against some valid and invalid examples.
#
# Prerequisites:
#
# 1. Install bats-core, see:
#    https://bats-core.readthedocs.io/en/stable/installation.html
#
# 2. Clone libraries into dev-support/ci:
#    cd dev-support/ci
#    git clone https://github.com/bats-core/bats-assert
#    git clone https://github.com/bats-core/bats-support
#
# Usage:
#    bats dev-support/ci/pr_title_check.bats

load bats-support/load.bash
load bats-assert/load.bash

@test "check legal PR title examples" {
  # 1 digit Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-1. Hello World'
  assert_output 'OK'

  # 2 digits Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-12. Hello World'
  assert_output 'OK'

  # 3 digits Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-123. Hello World'
  assert_output 'OK'

  # 4 digits Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello World'
  assert_output 'OK'

  # 5 digits Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-12345. Hello World'
  assert_output 'OK'

  # PR with tag
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. [Tag] Hello World'
  assert_output 'OK'

  # trailing dot is allowed
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello World.'
  assert_output 'OK'

  # case in summary does not matter
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. hello world in lower case'
  assert_output 'OK'
}

@test "check illegal PR title examples" {
  # HDDS case matters
  run dev-support/ci/pr_title_check.sh 'Hdds-1234. Hello World'
  assert_output 'Fail: must start with HDDS'

  # missing dash in Jira
  run dev-support/ci/pr_title_check.sh 'HDDS 1234. Hello World'
  assert_output 'Fail: missing dash in Jira'

  # 6 digits Jira not needed yet 
  run dev-support/ci/pr_title_check.sh 'HDDS-123456. Hello World'
  assert_output 'Fail: Jira must be 1 to 5 digits'

  # leading zero in Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-01234. Hello World'
  assert_output 'Fail: leading zero in Jira'

  # missing dot after Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-1234 Hello World'
  assert_output 'Fail: missing dot after Jira'

  # missing space after Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-1234.Hello World'
  assert_output 'Fail: missing space after Jira'

  # trailing space
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello World '
  assert_output 'Fail: trailing space'

  # trailing ellipsis
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello World...'
  assert_output 'Fail: trailing ellipsis indicates title is cut'

  # trailing ellipsis
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello World…'
  assert_output 'Fail: trailing ellipsis indicates title is cut'

  # double spaces after Jira
  run dev-support/ci/pr_title_check.sh 'HDDS-1234.  Hello World'
  assert_output 'Fail: two consecutive spaces'

  # double spaces in summary
  run dev-support/ci/pr_title_check.sh 'HDDS-1234. Hello  World'
  assert_output 'Fail: two consecutive spaces'
}
