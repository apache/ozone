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

load bats-support/load.bash
load bats-assert/load.bash

@test "compose only" {
  run dev-support/ci/selective_ci_checks.sh 5e6f6fef9

  assert_output -p 'basic-checks=["rat"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "compose and robot" {
  run dev-support/ci/selective_ci_checks.sh b83039eef

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "integration only" {
  run dev-support/ci/selective_ci_checks.sh 61396ba9f

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=false
}

@test "kubernetes only" {
  run dev-support/ci/selective_ci_checks.sh 5336bb9bd

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "docs only" {
  run dev-support/ci/selective_ci_checks.sh 474457cb3

  assert_output -p 'basic-checks=["rat","docs"]'
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "java-only change" {
  run dev-support/ci/selective_ci_checks.sh 01c616536

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "java and compose change" {
  run dev-support/ci/selective_ci_checks.sh d0f0f806e

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "pom change" {
  run dev-support/ci/selective_ci_checks.sh 9129424a9

  assert_output -p 'basic-checks=["rat","checkstyle","findbugs","unit"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "CI env change" {
  run dev-support/ci/selective_ci_checks.sh ceb79acaa

  assert_output -p 'basic-checks=["author","bats","checkstyle","docs","findbugs","rat","unit"]'
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}
