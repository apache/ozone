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

#checks:unit

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CHECK=native

zlib_version=$(mvn -N help:evaluate -Dexpression=zlib.version -q -DforceStdout)
if [[ -z "${zlib_version}" ]]; then
  echo "ERROR zlib.version not defined in pom.xml"
  exit 1
fi

source "${DIR}/junit.sh" -Pnative -Drocks_tools_native \
  -Dzlib.url="https://github.com/madler/zlib/releases/download/v${zlib_version}/zlib-${zlib_version}.tar.gz" \
  -DexcludedGroups="unhealthy" \
  "$@"
