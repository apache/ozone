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

load ozone-functions_test_helper

@test "ozone_set_var_for_compatibility: keep values if both set" {
  export target_var="old value"
  export source_var="new value"

  run ozone_set_var_for_compatibility target_var source_var "message not printed"

  [[ -z "$output" ]]
  [[ "$target_var" == "old value" ]]
  [[ "$source_var" == "new value" ]]
}

@test "ozone_set_var_for_compatibility: keep values even if target is empty" {
  export target_var=""
  export source_var="new value"

  run ozone_set_var_for_compatibility target_var source_var "message not printed"

  [[ -z "$output" ]]
  [[ "$target_var" == "" ]]
  [[ "$source_var" == "new value" ]]
}

@test "ozone_set_var_for_compatibility: ignore unset source variable" {
  export target_var="old value"
  unset source_var

  run ozone_set_var_for_compatibility target_var source_var "message not printed"

  [[ -z "$output" ]]
  [[ "$target_var" == "old value" ]]
  [[ -z "${!source_var*}" ]]
}

@test "ozone_set_var_for_compatibility: set target if unset" {
  unset target_var
  export source_var="new value"

  run ozone_set_var_for_compatibility target_var source_var "expected message"

  [[ "$output" == "expected message" ]]
  [[ "$target_var" == "new value" ]]
  [[ "$source_var" == "new value" ]]
}

@test "ozone_set_var_for_compatibility: set target even if source is empty" {
  unset target_var
  export source_var=""

  run ozone_set_var_for_compatibility target_var source_var "expected message"

  [[ "$output" == "expected message" ]]
  [[ -n "${!target_var*}" ]] # exists
  [[ "$target_var" == "" ]]
  [[ -n "${!source_var*}" ]]
  [[ "$source_var" == "" ]]
}

@test "ozone_set_var_for_compatibility: allow both to be unset" {
  unset target_var source_var

  run ozone_set_var_for_compatibility target_var source_var "message not printed"

  [[ -z "$output" ]]
  [[ -z "${!target_var*}" ]]
  [[ -z "${!source_var*}" ]]
}
