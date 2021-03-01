#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Version that will be run using the local build.
: "${OZONE_CURRENT_VERSION:=1.1.0}"
export OZONE_CURRENT_VERSION

TEST_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
source "$TEST_DIR/testlib.sh"

# Export variables needed by tests.
export TEST_DIR
# Required by ../testlib.sh.
export COMPOSE_DIR="$TEST_DIR"

RESULT_DIR="$ALL_RESULT_DIR" create_results_dir

# Upgrade tests to be run.
# Run all upgrades even if one fails.
# Any failure will save a failing return code to $RESULT.
set +e
run_test manual-upgrade 0.5.0 1.1.0
run_test non-rolling-upgrade 1.0.0 1.1.0
set -e

generate_report "upgrade" "${ALL_RESULT_DIR}"

exit "$RESULT"
