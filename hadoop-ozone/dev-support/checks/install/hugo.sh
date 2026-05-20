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

# This script installs Hugo.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

: ${HUGO_VERSION:=0.152.2}

_install_hugo() {
  local os=$(uname -s)
  local arch=$(uname -m)

  mkdir -p bin

  case "${arch}" in
    x86_64)
      arch=amd64
      ;;
    aarch64)
      arch=arm64
      ;;
  esac

  case "${os}" in
    Darwin)
      os=darwin
      arch=universal
      ;;
    Linux)
      os=linux
      ;;
  esac

  curl -LSs "https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_${HUGO_VERSION}_${os}-${arch}.tar.gz" | tar -xz -f - -C bin hugo
  chmod +x bin/hugo
}

_install_tool hugo bin
