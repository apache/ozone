#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# files that should be cleaned up when the script exits
# shellcheck disable=SC2034
FILES_TO_CLEANUP_ON_EXIT=()

function initialization::initialize_git_variables() {
    # SHA of the commit for the current sources
    COMMIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo "Unknown")"
    export COMMIT_SHA
}

function initialization::set_output_color_variables() {
    COLOR_BLUE=$'\e[34m'
    COLOR_GREEN=$'\e[32m'
    COLOR_RED=$'\e[31m'
    COLOR_RESET=$'\e[0m'
    COLOR_YELLOW=$'\e[33m'
    COLOR_CYAN=$'\e[36m'
    export COLOR_BLUE
    export COLOR_GREEN
    export COLOR_RED
    export COLOR_RESET
    export COLOR_YELLOW
    export COLOR_CYAN
}

# Common environment that is initialized by CI scripts
function initialization::initialize_common_environment() {
    initialization::set_output_color_variables
    initialization::initialize_git_variables
}

function initialization::summarize_build_environment() {
    cat <<EOF

Configured build variables:

Git variables:

    COMMIT_SHA = ${COMMIT_SHA}

Verbosity variables:

    VERBOSE: ${VERBOSE}
    VERBOSE_COMMANDS: ${VERBOSE_COMMANDS}

EOF
    if [[ "${CI}" == "true" ]]; then
        cat <<EOF

Detected CI build environment:

    CI_TARGET_REPO=${CI_TARGET_REPO}
    CI_TARGET_BRANCH=${CI_TARGET_BRANCH}
    CI_BUILD_ID=${CI_BUILD_ID}
    CI_JOB_ID=${CI_JOB_ID}
    CI_EVENT_TYPE=${CI_EVENT_TYPE}
EOF
    fi
}

# Retrieves CI environment variables needed - depending on the CI system we run it in.
# We try to be CI - agnostic and our scripts should run the same way on different CI systems
# (This makes it easy to move between different CI systems)
# This function maps CI-specific variables into a generic ones (prefixed with CI_) that
# we used in other scripts
function initialization::get_environment_for_builds_on_ci() {
    if [[ ${CI:=} == "true" ]]; then
        export GITHUB_REPOSITORY="${GITHUB_REPOSITORY="apache/ozone"}"
        export CI_TARGET_REPO="${GITHUB_REPOSITORY}"
        export CI_TARGET_BRANCH="${GITHUB_BASE_REF:="master"}"
        export CI_BUILD_ID="${GITHUB_RUN_ID="0"}"
        export CI_JOB_ID="${GITHUB_JOB="0"}"
        export CI_EVENT_TYPE="${GITHUB_EVENT_NAME="pull_request"}"
        export CI_REF="${GITHUB_REF:="refs/head/master"}"
    else
        # CI PR settings
        export GITHUB_REPOSITORY="${GITHUB_REPOSITORY="apache/ozone"}"
        export CI_TARGET_REPO="${CI_TARGET_REPO="apache/ozone"}"
        export CI_TARGET_BRANCH="${DEFAULT_BRANCH="master"}"
        export CI_BUILD_ID="${CI_BUILD_ID="0"}"
        export CI_JOB_ID="${CI_JOB_ID="0"}"
        export CI_EVENT_TYPE="${CI_EVENT_TYPE="pull_request"}"
        export CI_REF="${CI_REF="refs/head/master"}"
    fi
}

# shellcheck disable=SC2034

# By the time this method is run, nearly all constants have been already set to the final values
# so we can set them as readonly.
function initialization::make_constants_read_only() {
    readonly VERBOSE

    readonly CI_BUILD_ID
    readonly CI_JOB_ID

    readonly GITHUB_REPOSITORY
}

# converts parameters to json array
function initialization::parameters_to_json() {
    echo -n "["
    local separator=""
    local var
    for var in "${@}"; do
        echo -n "${separator}\"${var}\""
        separator=","
    done
    echo "]"
}

# output parameter name and value - both to stdout and to be set by GitHub Actions
function initialization::ga_output() {
    if [[ -n "${GITHUB_OUTPUT=}" ]]; then
        echo "${1}=${2}" >> "${GITHUB_OUTPUT}"
    fi
    echo "${1}=${2}"
}

function initialization::ga_env() {
    if [[ -n "${GITHUB_ENV=}" ]]; then
        echo "${1}=${2}" >>"${GITHUB_ENV}"
    fi
}
