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

# Function to find project paths for a given test class
find_project_paths_for_test_class() {
  local test_class="$1"

  if [[ -z "${test_class}" ]] || [[ "${test_class}" == "Abstract"* ]]; then
    return
  fi

  echo "Finding project for test class: ${test_class}" >&2

  # Trim test_class of whitespace
  local base_class_name=$(echo "${test_class}" | xargs)

  # If the base name is empty after removing wildcards, use a reasonable default
  if [[ -z "${base_class_name}" ]]; then
    echo "Test class name contains only wildcards, searching in all test directories" >&2
    return
  fi

  echo "Searching for files matching base name: ${base_class_name}" >&2

  # Find all projects containing matching test classes - use a more flexible search approach
  # First try direct filename search
  local test_files=($(find . -path "*/src/test/*" -name "${base_class_name}.java" | sort -u))

  # If no files found and the class name contains dots (package notation), try searching by path
  if [[ ${#test_files[@]} -eq 0 && "${base_class_name}" == *"."* ]]; then
    # Convert base class to path format
    local test_class_path="${base_class_name//./\/}.java"
    echo "No files found with direct name search, trying path-based search" >&2
    echo "TEST_CLASS_PATH pattern: ${test_class_path}" >&2

    # Search by path pattern
    test_files=($(find . -path "*/src/test/*/${test_class_path%.*}*.java" | sort -u))
  fi

  echo "Found ${#test_files[@]} matching test file(s)" >&2

  if [[ ${#test_files[@]} -gt 0 ]]; then
    # Extract project paths (up to the src/test directory)
    local project_paths=()
    for test_file in "${test_files[@]}"; do
      echo "TEST_FILE: ${test_file}" >&2
      local project_path=$(dirname "${test_file}" | sed -e 's|/src/test.*||')
      if [[ -f "${project_path}/pom.xml" ]]; then
        echo "Found test in project: ${project_path}" >&2
        project_paths+=("${project_path}")
      fi
    done

    printf '%s\n' "${project_paths[@]}" | sort -u
  else
    echo "Could not find project for test class pattern: ${test_class}" >&2
  fi
}

# Takes a project list which is the output of `find_project_paths_for_test_class`
# and returns a string that can use for maven -pl option, eg. "./project1\n./project2" -> "-pl ./project1,./project2"
build_maven_project_list() {
  local project_paths="$1"
  if [[ -z "${project_paths}" ]]; then
    echo ""
    return
  fi

  local comma_separated=$(echo "${project_paths}" | tr '\n' ',')
  comma_separated="${comma_separated%,}"
  echo "-pl ${comma_separated}"
}

# If option get-pl set, write the maven -pl option value to stdout
# otherwise, write the project paths to stdout
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [[ "$1" == "--get-pl" ]]; then
    shift
    project_paths=$(find_project_paths_for_test_class "$@")
    build_maven_project_list "${project_paths}"
  else
    find_project_paths_for_test_class "$@"
  fi
fi
