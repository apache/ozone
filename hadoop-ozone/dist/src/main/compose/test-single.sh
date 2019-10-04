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

#
# Single test executor, can start a single robot test in any running container.
#


COMPOSE_DIR="$PWD"
export COMPOSE_DIR

if [[ ! -f "$COMPOSE_DIR/docker-compose.yaml" ]]; then
    echo "docker-compose.yaml is missing from the current dir. Please run this command from a docker-compose environment."
    exit 1
fi
if (( $# != 2 )); then
cat << EOF
   Single test executor

   Usage:

     ../test-single.sh <container> <robot_test>

        container: Name of the running docker-compose container (docker-compose.yaml is required in the current directory)

        robot_test: name of the robot test or directory relative to the smoketest dir.



EOF

fi

# shellcheck source=testlib.sh
source "$COMPOSE_DIR/../testlib.sh"

create_results_dir

execute_robot_test "$1" "$2"

generate_report
