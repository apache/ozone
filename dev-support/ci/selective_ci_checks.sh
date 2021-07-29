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

# shellcheck source=dev-support/ci/lib/_script_init.sh
. dev-support/ci/lib/_script_init.sh

# Parameter:
#
# $1 - COMMIT SHA of the incoming commit. If this parameter is missing, this script does not check anything,
#      it simply sets all the version outputs that determine that all tests should be run.
#      This happens in case the event triggering the workflow is 'schedule' or 'push'.
#
# The logic of retrieving changes works by comparing the incoming commit with the target branch
# the commit addresses.
#
#
declare -a pattern_array ignore_array

function check_for_full_tests_needed_label() {
    if [[ ${PR_LABELS=} == *"full tests needed"* ]]; then
        echo
        echo "Found PR label 'full tests needed' in '${PR_LABELS=}'"
        echo
        FULL_TESTS_NEEDED_LABEL="true"
    else
        echo
        echo "Did not find PR label 'full tests needed' in '${PR_LABELS=}'"
        echo
        FULL_TESTS_NEEDED_LABEL="false"
    fi
}

function get_changed_files() {
    start_end::group_start "Get changed files"
    local base

    echo
    echo "Incoming commit SHA: ${INCOMING_COMMIT_SHA}"
    echo

    CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r \
        "${INCOMING_COMMIT_SHA}~1" "${INCOMING_COMMIT_SHA}" || true)
    if [[ -z "${CHANGED_FILES}" ]]; then
        echo
        echo "${COLOR_YELLOW}WARNING: Could not find any changed files${COLOR_RESET}"
        echo Assuming that we should run all tests in this case
        echo
        set_outputs_run_everything_and_exit
    fi
    echo
    echo "Changed files:"
    echo
    echo "${CHANGED_FILES}"
    echo
    readonly CHANGED_FILES
    start_end::group_end
}

function set_needs_build() {
    initialization::ga_output needs-build "${@}"
}

function set_needs_dependency_check() {
    initialization::ga_output needs-dependency-check "${@}"
}

function set_needs_basic_checks() {
    initialization::ga_output needs-basic-checks "${@}"
}

function set_needs_compose_tests() {
    initialization::ga_output needs-compose-tests "${@}"
}

function set_needs_integration_tests() {
    initialization::ga_output needs-integration-tests "${@}"
}

function set_needs_kubernetes_tests() {
    initialization::ga_output needs-kubernetes-tests "${@}"
}

function set_basic_checks() {
    initialization::ga_output basic-checks \
        "$(initialization::parameters_to_json "${@}")"
}

function set_outputs_run_everything_and_exit() {
    set_basic_checks author bats checkstyle docs findbugs rat unit
    set_needs_build "true"
    set_needs_basic_checks "true"
    set_needs_compose_tests "true"
    set_needs_dependency_check "true"
    set_needs_integration_tests "true"
    set_needs_kubernetes_tests "true"
    exit
}

function set_output_skip_all_tests_and_exit() {
    set_basic_checks
    set_needs_build "false"
    set_needs_basic_checks "false"
    set_needs_compose_tests "false"
    set_needs_dependency_check "false"
    set_needs_integration_tests "false"
    set_needs_kubernetes_tests "false"
    exit
}

# Converts array of patterns into single | pattern string
#    pattern_array - array storing regexp patterns
# Outputs - pattern string
function get_regexp_from_patterns() {
    local array_name=${1:-"pattern_array"}
    local test_triggering_regexp=""
    local separator=""
    local pattern
    local array="${array_name}[@]"
    for pattern in "${!array}"; do
        test_triggering_regexp="${test_triggering_regexp}${separator}${pattern}"
        separator="|"
    done
    echo "${test_triggering_regexp}"
}

# Shows changed files in the commit vs. the target.
# Input:
#    pattern_array - array storing regexp patterns
function show_changed_files() {
    local the_regexp
    match=$(get_regexp_from_patterns pattern_array)
    ignore=$(get_regexp_from_patterns ignore_array)
    echo
    echo "Changed files matching the '${match}' pattern, but ignoring '${ignore}':"
    echo
    echo "${CHANGED_FILES}" | grep -E "${match}" | grep -v "${ignore}" || true
    echo
}

# Counts changed files in the commit vs. the target
# Input:
#    pattern_array - array storing regexp patterns
# Output:
#    Count of changed files matching the patterns
function count_changed_files() {
    echo "${CHANGED_FILES}" | grep -c -E "$(get_regexp_from_patterns)" || true
}

SOURCES_TRIGGERING_TESTS=(
    "^hadoop-hdds"
    "^hadoop-ozone"
    "^pom.xml"
)
readonly SOURCES_TRIGGERING_TESTS

function check_if_tests_are_needed_at_all() {
    start_end::group_start "Check if tests are needed"

    if [[ "${PR_DRAFT}" == "true" ]]; then
        echo "Draft PR, skipping tests"
        set_output_skip_all_tests_and_exit
    fi

    local pattern_array=("${SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files

    if [[ $(count_changed_files) == "0" ]]; then
        echo "None of the important files changed, skipping tests"
        set_output_skip_all_tests_and_exit
    fi
    start_end::group_end
}

function run_all_tests_if_environment_files_changed() {
    start_end::group_start "Check if everything should be run"
    local pattern_array=(
        "^.github/workflows/"
        "^dev-support/ci"
        "^hadoop-ozone/dev-support/checks/_lib.sh"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        echo "Important environment files changed. Running everything"
        set_outputs_run_everything_and_exit
    fi
    if [[ ${FULL_TESTS_NEEDED_LABEL} == "true" ]]; then
        echo "Full tests requested by label on PR. Running everything"
        set_outputs_run_everything_and_exit
    fi
    start_end::group_end
}

function get_count_all_files() {
    start_end::group_start "Count all changed source files"
    local pattern_array=("${SOURCES_TRIGGERING_TESTS[@]}")
    show_changed_files
    COUNT_ALL_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_ALL_CHANGED_FILES}"
    readonly COUNT_ALL_CHANGED_FILES
    start_end::group_end
}

function get_count_compose_files() {
    start_end::group_start "Count compose files"
    local pattern_array=(
        "^hadoop-ozone/dist/src/main/compose"
    )
    show_changed_files
    COUNT_COMPOSE_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_COMPOSE_CHANGED_FILES}"
    readonly COUNT_COMPOSE_CHANGED_FILES
    start_end::group_end
}

function get_count_doc_files() {
    start_end::group_start "Count doc files"
    local pattern_array=(
        "^hadoop-hdds/docs"
        "^hadoop-ozone/dev-support/checks/docs.sh"
    )
    local ignore_array=(
        "^hadoop-hdds/docs/README.md"
    )
    show_changed_files
    COUNT_DOC_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_DOC_CHANGED_FILES}"
    readonly COUNT_DOC_CHANGED_FILES
    start_end::group_end
}

function get_count_ignored_files() {
    start_end::group_start "Count ignored files"
    local pattern_array=(
        "/README.md" # exclude root README
    )
    show_changed_files
    COUNT_IGNORED_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_IGNORED_CHANGED_FILES}"
    readonly COUNT_IGNORED_CHANGED_FILES
    start_end::group_end
}

function get_count_junit_files() {
    start_end::group_start "Count junit test files"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/_mvn_unit_report.sh"
        "^hadoop-ozone/dev-support/checks/integration.sh"
        "^hadoop-ozone/dev-support/checks/unit.sh"
        "src/test/java"
        "src/test/resources"
    )
    show_changed_files
    COUNT_JUNIT_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_JUNIT_CHANGED_FILES}"
    readonly COUNT_JUNIT_CHANGED_FILES
    start_end::group_end
}

function get_count_kubernetes_files() {
    start_end::group_start "Count kubernetes files"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
        "^hadoop-ozone/dist/src/main/k8s"
    )
    show_changed_files
    COUNT_KUBERNETES_CHANGED_FILES=$(count_changed_files)
    echo "Files count: ${COUNT_KUBERNETES_CHANGED_FILES}"
    readonly COUNT_KUBERNETES_CHANGED_FILES
    start_end::group_end
}

function check_needs_bats() {
    start_end::group_start "Check if bats is needed"
    local pattern_array=(
        "\.bash$"
        "\.bats$"
        "\.sh$" # includes hadoop-ozone/dev-support/checks/bats.sh
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} bats"
    fi

    start_end::group_end
}

function check_needs_checkstyle() {
    start_end::group_start "Check if checkstyle is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/checkstyle.sh"
        "^hadoop-hdds/dev-support/checkstyle"
        "pom.xml"
        "src/..../java"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} checkstyle"
    fi

    start_end::group_end
}

function check_needs_docs() {
    start_end::group_start "Check if docs is needed"

    if [[ ${COUNT_DOC_CHANGED_FILES} != "0" ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} docs"
    fi

    start_end::group_end
}

function check_needs_dependency() {
    start_end::group_start "Check if dependency is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/dependency.sh"
        "^hadoop-ozone/dist/src/main/license/update-jar-report.sh"
        "^hadoop-ozone/dist/src/main/license/jar-report.txt"
        "pom.xml"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        set_needs_dependency_check "true"
    else
        set_needs_dependency_check "false"
    fi

    start_end::group_end
}

function check_needs_findbugs() {
    start_end::group_start "Check if findbugs is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/findbugs.sh"
        "findbugsExcludeFile.xml"
        "pom.xml"
        "src/..../java"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} findbugs"
    fi

    start_end::group_end
}

function check_needs_unit_test() {
    start_end::group_start "Check if unit test is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/_mvn_unit_report.sh"
        "^hadoop-ozone/dev-support/checks/unit.sh"
        "pom.xml"
        "src/..../java"
        "src/..../resources"
    )
    show_changed_files

    if [[ $(count_changed_files) != "0" ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} unit"
    fi

    start_end::group_end
}

function calculate_test_types_to_run() {
    start_end::group_start "Count core/other files"
    COUNT_CORE_OTHER_CHANGED_FILES=$((COUNT_ALL_CHANGED_FILES - COUNT_COMPOSE_CHANGED_FILES - COUNT_DOC_CHANGED_FILES - COUNT_IGNORED_CHANGED_FILES - COUNT_JUNIT_CHANGED_FILES - COUNT_KUBERNETES_CHANGED_FILES))
    readonly COUNT_CORE_OTHER_CHANGED_FILES
    echo
    echo "Files count: ${COUNT_CORE_OTHER_CHANGED_FILES}"
    echo

    local compose_tests_needed=false
    local integration_tests_needed=false
    local kubernetes_tests_needed=false

    if [[ ${COUNT_CORE_OTHER_CHANGED_FILES} -gt 0 ]]; then
        # Running all tests because some core or other files changed
        echo
        echo "Looks like ${COUNT_CORE_OTHER_CHANGED_FILES} core files changed, running all tests."
        echo
        compose_tests_needed=true
        integration_tests_needed=true
        kubernetes_tests_needed=true
    else
        if [[ ${COUNT_COMPOSE_CHANGED_FILES} != "0" ]]; then
            compose_tests_needed="true"
        fi
        if [[ ${COUNT_JUNIT_CHANGED_FILES} != "0" ]]; then
            integration_tests_needed="true"
        fi
        if [[ ${COUNT_KUBERNETES_CHANGED_FILES} != "0" ]]; then
            kubernetes_tests_needed="true"
        fi
    fi

    if [[ "${compose_tests_needed}" == "true" ]] || [[ "${kubernetes_tests_needed}" == "true" ]]; then
        set_needs_build "true"
    fi

    set_needs_compose_tests "${compose_tests_needed}"
    set_needs_integration_tests "${integration_tests_needed}"
    set_needs_kubernetes_tests "${kubernetes_tests_needed}"

    start_end::group_end
}

initialization::summarize_build_environment

check_for_full_tests_needed_label

if (($# < 1)); then
    echo
    echo "No Commit SHA - running all tests (likely direct merge, or scheduled run)"
    echo
    INCOMING_COMMIT_SHA=""
    readonly INCOMING_COMMIT_SHA
    # override FULL_TESTS_NEEDED_LABEL in main/scheduled run
    FULL_TESTS_NEEDED_LABEL="true"
    readonly FULL_TESTS_NEEDED_LABEL
    set_outputs_run_everything_and_exit
else
    INCOMING_COMMIT_SHA="${1}"
    readonly INCOMING_COMMIT_SHA
    readonly FULL_TESTS_NEEDED_LABEL
fi


get_changed_files
run_all_tests_if_environment_files_changed
check_if_tests_are_needed_at_all

get_count_all_files
get_count_compose_files
get_count_doc_files
get_count_ignored_files
get_count_junit_files
get_count_kubernetes_files

# calculate basic checks to run
BASIC_CHECKS="author rat"
check_needs_bats
check_needs_checkstyle
check_needs_dependency
check_needs_docs
check_needs_findbugs
check_needs_unit_test
set_needs_basic_checks "true"
set_basic_checks ${BASIC_CHECKS}

calculate_test_types_to_run
