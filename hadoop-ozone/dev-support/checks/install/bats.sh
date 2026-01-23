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

# This script installs bats.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

: ${BATS_VERSION:="1.2.1"}

_install_bats() {
  curl -LSs "https://github.com/bats-core/bats-core/archive/v${BATS_VERSION}.tar.gz" | tar -xz -f -
}

_install_tool bats "bats-core-${BATS_VERSION}/bin"

git clone https://github.com/bats-core/bats-assert dev-support/ci/bats-assert
git clone https://github.com/bats-core/bats-support dev-support/ci/bats-support
