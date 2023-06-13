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
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

: ${OZONE_WITH_COVERAGE:="false"}

MAVEN_OPTIONS='-V -B -Dmaven.javadoc.skip=true -DskipTests -DskipDocs --no-transfer-progress'

if [[ "${OZONE_WITH_COVERAGE}" == "true" ]]; then
  MAVEN_OPTIONS="${MAVEN_OPTIONS} -Pcoverage"
else
  MAVEN_OPTIONS="${MAVEN_OPTIONS} -Djacoco.skip"
fi

if [[ "${SKIP_NATIVE_VERSION_CHECK}" != "true" ]]; then
  NATIVE_MAVEN_OPTIONS="-Drocks_tools_native"
  . "$DIR/native_check.sh"
  init_native_maven_opts
  MAVEN_OPTIONS="${MAVEN_OPTIONS} ${NATIVE_MAVEN_OPTIONS}"
fi
export MAVEN_OPTS="-Xmx4096m $MAVEN_OPTS"
echo "${MAVEN_OPTIONS}"
mvn ${MAVEN_OPTIONS} clean install "$@"
exit $?
