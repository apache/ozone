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
# In case "VERBOSE_COMMANDS" is set to "true" set -x is used to enable debugging

DOCKER_BINARY_PATH="${DOCKER_BINARY_PATH:=$(command -v docker || echo "/bin/docker")}"
export DOCKER_BINARY_PATH

function verbosity::store_exit_on_error_status() {
    exit_on_error="false"
    # If 'set -e' is set before entering the function, remember it, so you can restore before return!
    if [[ $- == *e* ]]; then
        exit_on_error="true"
    fi
    set +e
}

function verbosity::restore_exit_on_error_status() {
    if [[ ${exit_on_error} == "true" ]]; then
        set -e
    fi
    unset exit_on_error
}

# Prints verbose information in case VERBOSE variable is set
function verbosity::print_info() {
    if [[ ${VERBOSE:="false"} == "true" && ${PRINT_INFO_FROM_SCRIPTS} == "true" ]]; then
        echo "$@"
    fi
}

function verbosity::set_verbosity() {
    # whether verbose output should be produced
    export VERBOSE=${VERBOSE:="false"}

    # whether every bash statement should be printed as they are executed
    export VERBOSE_COMMANDS=${VERBOSE_COMMANDS:="false"}

    # whether the output from script should be printed at all
    export PRINT_INFO_FROM_SCRIPTS=${PRINT_INFO_FROM_SCRIPTS:="true"}
}

verbosity::set_verbosity
