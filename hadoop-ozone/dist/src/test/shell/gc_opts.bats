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

#
# Can be executed with bats (https://github.com/bats-core/bats-core)
# bats gc_opts.bats
#

load ozone-functions_test_helper

@test "Setting GC parameters: add GC params for server" {
  export OZONE_SUBCMD_SUPPORTDAEMONIZATION=true
  export OZONE_OPTS="Test"

  ozone_add_default_gc_opts

  echo $OZONE_OPTS
  [[ "$OZONE_OPTS" =~ "UseConcMarkSweepGC" ]]
}

@test "Setting GC parameters: disabled for client" {
  export OZONE_SUBCMD_SUPPORTDAEMONIZATION=false
  export OZONE_OPTS="Test"

  ozone_add_default_gc_opts

  [[ ! "$OZONE_OPTS" =~ "UseConcMarkSweepGC" ]]
}

@test "Setting GC parameters: disabled if GC params are customized" {
  export OZONE_SUBCMD_SUPPORTDAEMONIZATION=true
  export OZONE_OPTS="-XX:++UseG1GC -Xmx512"

  ozone_add_default_gc_opts

  [[ ! "$OZONE_OPTS" =~ "UseConcMarkSweepGC" ]]
}
