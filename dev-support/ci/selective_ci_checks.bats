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

# This script confirms that selective_ci_checks.sh works correctly
# against some known commits.
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
#    bats dev-support/ci/selective_ci_checks.bats

load bats-support/load.bash
load bats-assert/load.bash

@test "checkstyle and bats" {
  run dev-support/ci/selective_ci_checks.sh 11b098430

  assert_output -p 'basic-checks=["rat","bats","checkstyle"]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "compose only" {
  run dev-support/ci/selective_ci_checks.sh 5e6f6fef9

  assert_output -p 'basic-checks=["rat"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "compose and robot" {
  run dev-support/ci/selective_ci_checks.sh b83039eef

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "runner image update" {
  run dev-support/ci/selective_ci_checks.sh b95eeba82a

  assert_output -p 'basic-checks=["rat"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "check script" {
  run dev-support/ci/selective_ci_checks.sh 316899152

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "integration and unit: java change" {
  run dev-support/ci/selective_ci_checks.sh 9aebf6e25

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=false
}

@test "integration and unit: script change" {
  run dev-support/ci/selective_ci_checks.sh c6850484f

  assert_output -p 'basic-checks=["rat","bats","unit"]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=false
}

@test "unit only" {
  run dev-support/ci/selective_ci_checks.sh 1dd1d0ba3

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "unit helper" {
  run dev-support/ci/selective_ci_checks.sh 88383d1d5

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=false
}

@test "integration only" {
  run dev-support/ci/selective_ci_checks.sh 61396ba9f

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=false
}

@test "kubernetes only" {
  run dev-support/ci/selective_ci_checks.sh 5336bb9bd

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=true
}

@test "docs only" {
  run dev-support/ci/selective_ci_checks.sh 474457cb3

  assert_output -p 'basic-checks=["rat","docs"]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "main/java change" {
  run dev-support/ci/selective_ci_checks.sh 86a771dfe

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "..../java change" {
  run dev-support/ci/selective_ci_checks.sh 01c616536

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "java and compose change" {
  run dev-support/ci/selective_ci_checks.sh d0f0f806e

  assert_output -p 'basic-checks=["rat","author","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "java and docs change" {
  run dev-support/ci/selective_ci_checks.sh 2c0adac26

  assert_output -p 'basic-checks=["rat","author","checkstyle","docs","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "pom change" {
  run dev-support/ci/selective_ci_checks.sh 9129424a9

  assert_output -p 'basic-checks=["rat","checkstyle","findbugs","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "CI lib change" {
  run dev-support/ci/selective_ci_checks.sh ceb79acaa

  assert_output -p 'basic-checks=["author","bats","checkstyle","docs","findbugs","rat","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "CI workflow change" {
  run dev-support/ci/selective_ci_checks.sh 90a8d7c01

  assert_output -p 'basic-checks=["author","bats","checkstyle","docs","findbugs","rat","unit"]'
  assert_output -p needs-build=true
  assert_output -p needs-compile=true
  assert_output -p needs-compose-tests=true
  assert_output -p needs-dependency-check=true
  assert_output -p needs-integration-tests=true
  assert_output -p needs-kubernetes-tests=true
}

@test "root README" {
  run dev-support/ci/selective_ci_checks.sh 8bbbf3f7d

  assert_output -p 'basic-checks=[]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "ignored code" {
  run dev-support/ci/selective_ci_checks.sh ac8aee7f8

  assert_output -p 'basic-checks=[]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "PR-title workflow" {
  run dev-support/ci/selective_ci_checks.sh 4f0bd4ae3

  assert_output -p 'basic-checks=["rat","bats"]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}

@test "other README" {
  run dev-support/ci/selective_ci_checks.sh 5532981a7

  assert_output -p 'basic-checks=["rat"]'
  assert_output -p needs-build=false
  assert_output -p needs-compile=false
  assert_output -p needs-compose-tests=false
  assert_output -p needs-dependency-check=false
  assert_output -p needs-integration-tests=false
  assert_output -p needs-kubernetes-tests=false
}
