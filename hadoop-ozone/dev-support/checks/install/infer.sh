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

# This script installs Infer.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

: ${INFER_VERSION:=1.3.0}

_install_infer() {
  local os
  os=$(uname -s)
  local arch
  arch=$(uname -m)

  if [[ "${os}" == "Linux" ]]; then
    local infer_archive="infer-linux-x86_64-v${INFER_VERSION}.tar.xz"
  elif [[ "${os}" == "Darwin" ]] && [[ "${arch}" == "arm64" ]]; then
    local infer_archive="infer-osx-arm64-v${INFER_VERSION}.tar.xz"
  else
    echo "Unsupported platform: ${os} ${arch}" >&2
    exit 1
  fi

  local url="https://github.com/facebook/infer/releases/download/v${INFER_VERSION}/${infer_archive}"
  mkdir -p "infer-v${INFER_VERSION}"
  curl -LSs "${url}" | tar -xJ -C "infer-v${INFER_VERSION}" --strip-components=1 -f -
}

_install_tool infer "infer-v${INFER_VERSION}/bin" "infer" "infer"
