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
set -x
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_DIR="$(realpath "$DIR"/../../..)"
cd "$PROJECT_DIR" || exit 1

if [ ! "$SONAR_TOKEN" ]; then
  echo "SONAR_TOKEN environment variable should be set"
  exit 1
fi

: "${SONAR_MAVEN_PLUGIN_VERSION:=5.1.0.4751}"

mvn -V -B -DskipDocs -DskipShade -DskipTests -DskipRecon --no-transfer-progress \
  -Dsonar.coverage.jacoco.xmlReportPaths="$(pwd)/target/coverage/all.xml" \
  verify "org.sonarsource.scanner.maven:sonar-maven-plugin:${SONAR_MAVEN_PLUGIN_VERSION}:sonar"
