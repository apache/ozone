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

: ${TOOLS_DIR:=$(pwd)/.dev-tools} # directory for tools
: ${OZONE_PREFER_LOCAL_TOOL:=true} # skip install if tools are already available (eg. via package manager)

## @description  Install a dependency.  Only first argument is mandatory.
## @param name of the tool
## @param the directory for binaries, relative to the tool directory; added to PATH.
## @param the directory for the tool, relative to TOOLS_DIR
## @param name of the executable, for testing if it is already installed
## @param name of the function that performs actual installation steps
_install_tool() {
  local tool bindir dir bin func

  tool="$1"
  bindir="${2:-}"
  dir="${TOOLS_DIR}"/"${3:-"${tool}"}"
  bin="${4:-"${tool}"}"
  func="${5:-"_install_${tool}"}"

  if [[ "${OZONE_PREFER_LOCAL_TOOL}" == "true" ]] && which "$bin" >& /dev/null; then
    echo "Skip installing $bin, as it's already available on PATH."
    return
  fi

  if [[ ! -d "${dir}" ]]; then
    mkdir -pv "${dir}"
    pushd "${dir}"
    if eval "${func}"; then
      echo "Installed ${tool} in ${dir}"
    else
      echo "Failed to install ${tool}"
      exit 1
    fi
    popd
  fi

  if [[ -n "${bindir}" ]]; then
    bindir="${dir}"/"${bindir}"
    if [[ -d "${bindir}" ]]; then
      if [[ "${OZONE_PREFER_LOCAL_TOOL}" == "true" ]]; then
        export PATH="${PATH}:${bindir}"
      else
        export PATH="${bindir}:${PATH}"
      fi
    fi
  fi
}
