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

#suite:upgrade

set -u -o pipefail

TEST_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
source "$TEST_DIR/testlib.sh"

# Export variables needed by tests and ../testlib.sh.
export TEST_DIR
export COMPOSE_DIR="$TEST_DIR"

RESULT_DIR="$ALL_RESULT_DIR" create_results_dir

# Upgrade tests to be run. In CI we want to run just one set, but for a release
# we might advise the release manager to run the full matrix.

# This is the version of Ozone that should use the runner image to run the
# code that was built. Other versions will pull images from docker hub.
run_test ha     non-rolling-upgrade 2.1.0 "$OZONE_CURRENT_VERSION"
# run_test ha     non-rolling-upgrade 2.0.0 "$OZONE_CURRENT_VERSION"
#run_test non-ha non-rolling-upgrade 1.4.1 "$OZONE_CURRENT_VERSION"
#run_test ha     non-rolling-upgrade 1.4.1 "$OZONE_CURRENT_VERSION"
# run_test ha non-rolling-upgrade 1.4.0 "$OZONE_CURRENT_VERSION"
# run_test ha non-rolling-upgrade 1.3.0 "$OZONE_CURRENT_VERSION"
# run_test ha non-rolling-upgrade 1.2.1 "$OZONE_CURRENT_VERSION"
# run_test om-ha non-rolling-upgrade 1.1.0 "$OZONE_CURRENT_VERSION"

generate_report "upgrade" "$ALL_RESULT_DIR"

exit "$RESULT"
