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

create_aws_dir() {
  if [[ "${CI:-}" == "true" ]]; then
    export OZONE_VOLUME_OWNER=1000 # uid (from ozone-runner image)
    pushd hadoop-ozone/dist/target/ozone-*
    sudo mkdir .aws && sudo chmod 777 .aws && sudo chown ${OZONE_VOLUME_OWNER} .aws
    popd
  fi
}


# Function to find project paths for a given test class
# Returns an array of project paths in PROJECT_PATHS
find_project_paths_for_test_class() {
  local test_class="$1"
  # Reset the global PROJECT_PATHS array
  PROJECT_PATHS=()
  
  if [[ -z "${test_class}" ]] || [[ "${test_class}" == "Abstract"* ]]; then
    return
  fi
  
  echo "Finding project for test class: ${test_class}"

  # Extract the base part of the class name (before any wildcard)
  local base_class_name="${test_class%%\**}"
  base_class_name="${base_class_name%%\?*}"

  # If the base name is empty after removing wildcards, use a reasonable default
  if [[ -z "${base_class_name}" ]]; then
    echo "Test class name contains only wildcards, searching in all test directories"
    return
  fi
  
  echo "Searching for files matching base name: ${base_class_name}"

  # Find all projects containing matching test classes - use a more flexible search approach
  # First try direct filename search
  local test_files=($(find . -path "*/src/test/*" -name "${base_class_name}*.java" | sort -u))

  # If no files found and the class name contains dots (package notation), try searching by path
  if [[ ${#test_files[@]} -eq 0 && "${base_class_name}" == *"."* ]]; then
    # Convert base class to path format
    local test_class_path="${base_class_name//./\/}.java"
    echo "No files found with direct name search, trying path-based search"
    echo "TEST_CLASS_PATH pattern: ${test_class_path}"

    # Search by path pattern
    test_files=($(find . -path "*/src/test/*/${test_class_path%.*}*.java" | sort -u))
  fi

  echo "Found ${#test_files[@]} matching test file(s)"

  if [[ ${#test_files[@]} -gt 0 ]]; then
    # Extract project paths (up to the src/test directory)
    for test_file in "${test_files[@]}"; do
      echo "TEST_FILE: ${test_file}"
      local project_path=$(dirname "${test_file}" | sed -e 's|/src/test.*||')
      if [[ -f "${project_path}/pom.xml" ]]; then
        echo "Found test in project: ${project_path}"
        PROJECT_PATHS+=("${project_path}")
      fi
    done
  else
    echo "Could not find project for test class pattern: ${test_class}"
  fi
}
