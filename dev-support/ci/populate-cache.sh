#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../.." || exit 1

# install, but skip shaded modules
./mvnw --batch-mode --show-version clean install \
  -DskipShade -Pgo-offline ${BUILD_ARGS:-} "$@"

# only compile, no shade
hadoop-ozone/dev-support/checks/_build.sh compile cyclonedx:makeBom \
  -pl :ozone-filesystem-shaded,:ozone-filesystem-hadoop2 \
  -Dmaven-replacer-plugin.skip "$@"

# exercise some checks on limited modules to get more dependencies
hadoop-ozone/dev-support/checks/errorprone.sh -pl :ozone-main "$@" || true
hadoop-ozone/dev-support/checks/pmd.sh -am -pl :ozone-dev-support "$@" || true
hadoop-ozone/dev-support/checks/repro.sh -pl :ozone-main "$@" || true
