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

set -eu -o pipefail

export K8S_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$K8S_DIR"

# shellcheck source=/dev/null
source "../testlib.sh"

pre_run_setup

export SCM=scm-0

execute_robot_test ${SCM} -v PREFIX:pre smoketest/freon/generate.robot
execute_robot_test ${SCM} -v PREFIX:pre smoketest/freon/validate.robot

# restart datanodes
kubectl delete pod datanode-0 datanode-1 datanode-2

wait_for_startup
wait_for_pipeline

execute_robot_test ${SCM} -v PREFIX:pre smoketest/freon/validate.robot
execute_robot_test ${SCM} -v PREFIX:post smoketest/freon/generate.robot
execute_robot_test ${SCM} -v PREFIX:post smoketest/freon/validate.robot
