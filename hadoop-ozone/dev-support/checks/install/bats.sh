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

# This script installs bats with bats-support and bats-assert libraries.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

: ${BATS_VERSION:="1.14.0"}
: ${BATS_SUPPORT_VERSION:="0.3.0"}
: ${BATS_ASSERT_VERSION:="2.2.4"}

_install_bats() {
  curl -LSs "https://github.com/bats-core/bats-core/archive/v${BATS_VERSION}.tar.gz" | tar -xz -f - \
    && curl -LSs "https://github.com/bats-core/bats-support/archive/v${BATS_SUPPORT_VERSION}.tar.gz" | tar -xz -f - \
    && curl -LSs "https://github.com/bats-core/bats-assert/archive/v${BATS_ASSERT_VERSION}.tar.gz" | tar -xz -f - \
    && rm -f bats-assert bats-support \
    && ln -s bats-support-${BATS_SUPPORT_VERSION} bats-support \
    && ln -s bats-assert-${BATS_ASSERT_VERSION} bats-assert
}

_install_tool bats "bats-core-${BATS_VERSION}/bin"
