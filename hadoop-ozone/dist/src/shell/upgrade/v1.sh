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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

: "${SCM_DIR:="${OZONE_VOLUME}/scm"}"
: "${OZONE_RUNNER_VERSION:="20200625-1"}"

docker run --rm -v "${SCM_DIR}":/scm -v "${SCRIPT_DIR}/v1":/upgrade -w /scm/metadata apache/ozone-runner:"${OZONE_RUNNER_VERSION}" /upgrade/01-migrate-scm-db.sh
