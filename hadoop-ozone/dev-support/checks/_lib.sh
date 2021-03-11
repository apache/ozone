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

check_name="$(basename "${BASH_SOURCE[1]}")"
check_name="${check_name%.sh}"

install_tool() {
  if [[ ! -d ${TOOL_DIR} ]]; then
    mkdir -pv "${TOOL_DIR}"
    pushd "${TOOL_DIR}"
    _install_tool_callback
    popd
  fi
}

: ${TOOLS_DIR:=$(pwd)/.dev-tools}
: ${TOOL_DIR:=${TOOLS_DIR}/${check_name}}
