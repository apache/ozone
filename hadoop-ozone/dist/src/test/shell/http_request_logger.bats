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
# bats http_request_logger.bats
#

load ozone-functions_test_helper

@test "HTTP request logger: value is propagated to OZONE_OPTS" {
  export OZONE_HTTP_REQUEST_LOGGER="ERROR,console"
  export OZONE_OPTS=""

  ozone_finalize_opts

  echo "$OZONE_OPTS"
  [[ "$OZONE_OPTS" =~ "-Dozone.http.request.logger=ERROR,console" ]]
}

@test "HTTP request logger: defaults are set by ozone_basic_init" {
  unset OZONE_HTTP_REQUEST_LOGGER
  unset OZONE_DAEMON_HTTP_REQUEST_LOGGER

  ozone_basic_init

  [[ "$OZONE_HTTP_REQUEST_LOGGER" == "INFO,console" ]]
  [[ "$OZONE_DAEMON_HTTP_REQUEST_LOGGER" == "INFO,HttpAccess" ]]
}
