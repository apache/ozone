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

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR
basename=$(basename ${COMPOSE_DIR})

# shellcheck source=hadoop-ozone/dist/src/main/compose/xcompat/lib.sh
source "${COMPOSE_DIR}/lib.sh"

# shellcheck source=hadoop-ozone/dist/src/main/compose/testlib.sh
source "${COMPOSE_DIR}/../testlib.sh"

current_version="${OZONE_CURRENT_VERSION}"
# TODO: debug acceptance test failures for client versions 1.0.0 on secure clusters
old_versions="1.1.0 1.2.1 1.3.0 1.4.1 2.0.0 2.1.2 2.2.2" # container is needed for each version in clients.yaml

export SECURITY_ENABLED=true
: ${OZONE_BUCKET_KEY_NAME:=key1}

echo 'Compatibility Test' > "${TEST_DATA_DIR}"/small

create_results_dir
