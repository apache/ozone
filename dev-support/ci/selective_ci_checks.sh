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
# This script gets a list of the changed files from git, and matches
# that list against a set of regexes, each of which corresponds to a
# test group.  For each set of matches, a flag is set to let github
# actions know to run that test group.

# The code is heavily based on this:
# https://github.com/apache/airflow/blob/6af963c7d5ae9b59d17b156a053d5c85e678a3cb/scripts/ci/selective_ci_checks.sh

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

matched_files=""
ignore_array=(
    "^[^/]*.md" # exclude root docs
)

function check_for_full_tests_needed_label() {
    start_end::group_start "Check for PR label"
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
    start_end::group_end
}

function get_changed_files() {
    start_end::group_start "Get changed files"

    echo
    echo "Incoming commit SHA: ${INCOMING_COMMIT_SHA}"
    echo

    CHANGED_FILES=$(git diff-tree --no-commit-id --name-only -r \
        "${INCOMING_COMMIT_SHA}~1" "${INCOMING_COMMIT_SHA}" || true)
    if [[ -z "${CHANGED_FILES}" ]]; then
        echo "${COLOR_YELLOW}WARNING: Could not find any changed files${COLOR_RESET}"
        echo Assuming that we should run all tests in this case
        set_outputs_run_everything_and_exit
    fi
    echo "Changed files:"
    echo
    echo "${CHANGED_FILES}"
    echo
    readonly CHANGED_FILES
    start_end::group_end
}

function set_outputs_run_everything_and_exit() {
    BASIC_CHECKS=$(grep -lr '^#checks:' hadoop-ozone/dev-support/checks \
                       | sort -u | xargs -n1 basename \
                       | cut -f1 -d'.')
    compile_needed=true
    compose_tests_needed=true
    dependency_check_needed=true
    integration_tests_needed=true
    kubernetes_tests_needed=true

    start_end::group_end
    set_outputs
    exit
}

function set_output_skip_all_tests_and_exit() {
    BASIC_CHECKS=""
    compose_tests_needed=false
    dependency_check_needed=false
    integration_tests_needed=false
    kubernetes_tests_needed=false

    start_end::group_end
    set_outputs
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

# Finds changed files in the commit that match the pattern given in
# `pattern_array` (but do not match `ignore_array`).
#
# Optionally also keeps a running list of matched files.  These matches
# are handled by one or more specific checks.
#
# Parameter:
#    add_to_list - if true, the files matched are added to the global
#        matched_files list
# Input:
#    pattern_array - array storing regexp patterns
#    ignore_array - array storing regexp patterns to be ignored
# Output:
#    match_count - number of matched files
#    matched_files (appended) - list of matched files
function filter_changed_files() {
    local add_to_list="${1:-}"

    local match ignore
    match=$(get_regexp_from_patterns pattern_array)
    ignore=$(get_regexp_from_patterns ignore_array)

    verbosity::store_exit_on_error_status

    match_count=$(echo "${CHANGED_FILES}" | grep -E "${match}" | grep -cEv "${ignore}")

    if [[ "${add_to_list}" == "true" ]]; then
        local additional=$(echo "${CHANGED_FILES}" | grep -E "${match}" | grep -Ev "${ignore}")
        matched_files="${matched_files}$(echo -e "\n${additional}")"
    fi

    echo
    echo "${match_count} changed files matching the '${match}' pattern, but ignoring '${ignore}':"
    echo
    echo "${CHANGED_FILES}" | grep -E "${match}" | grep -Ev "${ignore}"
    echo

    verbosity::restore_exit_on_error_status
}

SOURCES_TRIGGERING_TESTS=(
    "^.github"
    "^dev-support"
    "^hadoop-hdds"
    "^hadoop-ozone"
    "^pom.xml"
)
readonly SOURCES_TRIGGERING_TESTS

function check_if_tests_are_needed_at_all() {
    start_end::group_start "Check if tests are needed"

    if [[ "${PR_DRAFT:-}" == "true" ]]; then
        echo "Draft PR, skipping tests"
        set_output_skip_all_tests_and_exit
    fi

    local pattern_array=("${SOURCES_TRIGGERING_TESTS[@]}")
    filter_changed_files

    if [[ ${match_count} == "0" ]]; then
        echo "None of the important files changed, skipping tests"
        set_output_skip_all_tests_and_exit
    fi
    start_end::group_end
}

function run_all_tests_if_environment_files_changed() {
    start_end::group_start "Check if everything should be run"
    local pattern_array=(
        "^.github/workflows/ci.yml"
        "^.github/workflows/post-commit.yml"
        "^dev-support/ci"
        "^hadoop-ozone/dev-support/checks/_lib.sh"
    )
    local ignore_array=(
        "^dev-support/ci/pr_title_check"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
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
    filter_changed_files
    COUNT_ALL_CHANGED_FILES=${match_count}
    readonly COUNT_ALL_CHANGED_FILES
    start_end::group_end
}

function get_count_compose_files() {
    start_end::group_start "Count compose files"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/acceptance.sh"
        "^hadoop-ozone/dist"
    )
    local ignore_array=(
        "^hadoop-ozone/dist/src/main/k8s"
        "^hadoop-ozone/dist/src/main/license"
        "\.md$"
    )
    filter_changed_files true
    COUNT_COMPOSE_CHANGED_FILES=${match_count}
    readonly COUNT_COMPOSE_CHANGED_FILES
    start_end::group_end
}

function get_count_doc_files() {
    start_end::group_start "Count doc files"
    local pattern_array=(
        "^hadoop-hdds/docs"
        "^hadoop-ozone/dev-support/checks/docs.sh"
    )
    filter_changed_files true
    COUNT_DOC_CHANGED_FILES=${match_count}
    readonly COUNT_DOC_CHANGED_FILES
    start_end::group_end
}

function get_count_integration_files() {
    start_end::group_start "Count integration test files"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/_mvn_unit_report.sh"
        "^hadoop-ozone/dev-support/checks/integration.sh"
        "^hadoop-ozone/dev-support/checks/junit.sh"
        "^hadoop-ozone/integration-test"
        "^hadoop-ozone/fault-injection-test/mini-chaos-tests"
        "src/test/java"
    )
    # Ozone's unit test naming convention: Test*.java
    # The following makes this filter ignore all tests except those in
    # integration-test and fault-injection-test.
    # Directories starting with `i` under hadoop-ozone need to be listed
    # explicitly, other subdirectories are captured by the second item.
    local ignore_array=(
        "^hadoop-hdds/.*/src/test/java/.*/Test.*.java"
        "^hadoop-ozone/[a-eghj-z].*/src/test/java/.*/Test.*.java"
        "^hadoop-ozone/insight/src/test/java/.*/Test.*.java"
        "^hadoop-ozone/interface-client/src/test/java/.*/Test.*.java"
        "^hadoop-ozone/interface-storage/src/test/java/.*/Test.*.java"
    )
    filter_changed_files true
    COUNT_INTEGRATION_CHANGED_FILES=${match_count}
    readonly COUNT_INTEGRATION_CHANGED_FILES
    start_end::group_end
}

function get_count_kubernetes_files() {
    start_end::group_start "Count kubernetes files"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
        "^hadoop-ozone/dist"
    )
    local ignore_array=(
        "^hadoop-ozone/dist/src/main/compose"
        "^hadoop-ozone/dist/src/main/license"
        "\.md$"
    )
    filter_changed_files true
    COUNT_KUBERNETES_CHANGED_FILES=${match_count}
    readonly COUNT_KUBERNETES_CHANGED_FILES
    start_end::group_end
}

function get_count_robot_files() {
    start_end::group_start "Count robot test files"
    local pattern_array=(
        "^hadoop-ozone/dist/src/main/smoketest"
    )
    filter_changed_files true
    COUNT_ROBOT_CHANGED_FILES=${match_count}
    readonly COUNT_ROBOT_CHANGED_FILES
    start_end::group_end
}

function check_needs_build() {
    start_end::group_start "Check if build is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/build.sh"
        "^hadoop-ozone/dev-support/checks/native_check.sh"
        "src/main/java"
        "src/main/resources"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        build_needed=true
    fi

    start_end::group_end
}

function check_needs_compile() {
    start_end::group_start "Check if compile is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/build.sh"
        "src/..../java"
        "src/..../proto"
        "pom.xml"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        compile_needed=true
    fi

    start_end::group_end
}

function check_needs_author() {
    start_end::group_start "Check if author is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/author.sh"
        "src/..../java"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        add_basic_check author
    fi

    start_end::group_end
}

function check_needs_bats() {
    start_end::group_start "Check if bats is needed"
    local pattern_array=(
        "\.bash$"
        "\.bats$"
        "\.sh$" # includes hadoop-ozone/dev-support/checks/bats.sh
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        add_basic_check bats
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
    local ignore_array=(
        "^hadoop-ozone/dist"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        add_basic_check checkstyle
    fi

    start_end::group_end
}

function check_needs_docs() {
    if [[ ${COUNT_DOC_CHANGED_FILES} != "0" ]]; then
        add_basic_check docs
    fi
}

function check_needs_dependency() {
    start_end::group_start "Check if dependency is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/dependency.sh"
        "^hadoop-ozone/dist/src/main/license/update-jar-report.sh"
        "^hadoop-ozone/dist/src/main/license/jar-report.txt"
        "pom.xml"
    )
    filter_changed_files

    dependency_check_needed=false
    if [[ ${match_count} != "0" ]]; then
        dependency_check_needed=true
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
    local ignore_array=(
        "^hadoop-ozone/dist"
    )
    filter_changed_files

    if [[ ${match_count} != "0" ]]; then
        add_basic_check findbugs
    fi

    start_end::group_end
}

function check_needs_native() {
    start_end::group_start "Check if native is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/native.sh"
        "^hadoop-hdds/rocks-native"
        # include tests tagged as @Native in any module
        $(grep -Flr 'org.apache.ozone.test.tag.Native' hadoop-*/*/src/test/java)
    )
    filter_changed_files true

    if [[ ${match_count} != "0" ]]; then
        add_basic_check native
    fi

    start_end::group_end
}

function check_needs_unit_test() {
    start_end::group_start "Check if unit test is needed"
    local pattern_array=(
        "^hadoop-ozone/dev-support/checks/_mvn_unit_report.sh"
        "^hadoop-ozone/dev-support/checks/unit.sh"
        "^hadoop-ozone/dev-support/checks/junit.sh"
        "src/test/java"
        "src/test/resources"
    )
    local ignore_array=(
        "^hadoop-ozone/dist"
        "^hadoop-ozone/fault-injection-test/mini-chaos-tests"
        "^hadoop-ozone/integration-test"
    )
    filter_changed_files true

    if [[ ${match_count} != "0" ]]; then
        add_basic_check unit
    fi

    start_end::group_end
}

# Counts other files which do not need to trigger any functional test
# (i.e. no compose/integration/kubernetes)
function get_count_misc_files() {
    start_end::group_start "Count misc. files"
    local pattern_array=(
        "^dev-support/ci/pr_title_check"
        "^.github"
        "^hadoop-hdds/dev-support/checkstyle"
        "^hadoop-ozone/dev-support/checks"
        "^hadoop-ozone/dist/src/main/license"
        "\.bats$"
        "\.txt$"
        "\.md$"
        "findbugsExcludeFile.xml"
        "/NOTICE$"
    )
    local ignore_array=(
        "^.github/workflows/post-commit.yml"
        "^hadoop-ozone/dev-support/checks/_mvn_unit_report.sh"
        "^hadoop-ozone/dev-support/checks/acceptance.sh"
        "^hadoop-ozone/dev-support/checks/integration.sh"
        "^hadoop-ozone/dev-support/checks/kubernetes.sh"
    )
    filter_changed_files true
    start_end::group_end
}

function add_basic_check() {
    local check="$1"
    if [[ "$BASIC_CHECKS" != *${check}* ]]; then
        BASIC_CHECKS="${BASIC_CHECKS} ${check}"
    fi
}

function calculate_test_types_to_run() {
    start_end::group_start "Count core/other files"
    verbosity::store_exit_on_error_status
    local matched_files_count=$(echo "${matched_files}" | sort -u | grep -cv "^$")
    verbosity::restore_exit_on_error_status
    COUNT_CORE_OTHER_CHANGED_FILES=$((COUNT_ALL_CHANGED_FILES - matched_files_count))
    readonly COUNT_CORE_OTHER_CHANGED_FILES

    compose_tests_needed=false
    integration_tests_needed=false
    kubernetes_tests_needed=false

    if [[ ${COUNT_CORE_OTHER_CHANGED_FILES} -gt 0 ]]; then
        # Running all tests because some core or other files changed
        echo "Looks like ${COUNT_CORE_OTHER_CHANGED_FILES} core files changed, running all tests."
        echo
        compose_tests_needed=true
        integration_tests_needed=true
        kubernetes_tests_needed=true
        add_basic_check unit
    else
        echo "All ${COUNT_ALL_CHANGED_FILES} changed files are known to be handled by specific checks."
        echo
        if [[ ${COUNT_COMPOSE_CHANGED_FILES} != "0" ]] || [[ ${COUNT_ROBOT_CHANGED_FILES} != "0" ]]; then
            compose_tests_needed="true"
        fi
        if [[ ${COUNT_INTEGRATION_CHANGED_FILES} != "0" ]]; then
            integration_tests_needed="true"
        fi
        if [[ ${COUNT_KUBERNETES_CHANGED_FILES} != "0" ]] || [[ ${COUNT_ROBOT_CHANGED_FILES} != "0" ]]; then
            kubernetes_tests_needed="true"
        fi
    fi
    start_end::group_end
}

function set_outputs() {
    # print results outside the group to increase visibility

    initialization::ga_output basic-checks \
        "$(initialization::parameters_to_json ${BASIC_CHECKS})"

    : ${compile_needed:=false}

    if [[ "${compile_needed}" == "true" ]] ||  [[ "${compose_tests_needed}" == "true" ]] || [[ "${kubernetes_tests_needed}" == "true" ]]; then
        build_needed=true
    fi

    initialization::ga_output needs-build "${build_needed:-false}"
    initialization::ga_output needs-compile "${compile_needed}"
    initialization::ga_output needs-compose-tests "${compose_tests_needed}"
    initialization::ga_output needs-dependency-check "${dependency_check_needed}"
    initialization::ga_output needs-integration-tests "${integration_tests_needed}"
    initialization::ga_output needs-kubernetes-tests "${kubernetes_tests_needed}"
}

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
check_if_tests_are_needed_at_all
run_all_tests_if_environment_files_changed

get_count_all_files
get_count_compose_files
get_count_doc_files
get_count_integration_files
get_count_kubernetes_files
get_count_robot_files
get_count_misc_files

check_needs_build
check_needs_compile

# calculate basic checks to run
BASIC_CHECKS="rat"
check_needs_author
check_needs_bats
check_needs_checkstyle
check_needs_dependency
check_needs_docs
check_needs_findbugs
check_needs_native
check_needs_unit_test
calculate_test_types_to_run
set_outputs
