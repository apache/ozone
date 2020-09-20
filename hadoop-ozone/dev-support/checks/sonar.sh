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

#Workaround: Sonar expects per-project Sonar XML report, but we have one, combined. Sonar seems to handle it well.
# Only the classes from the current project will be used. We can copy the same, combined report to all the subprojects.
if [ -f "$PROJECT_DIR/target/coverage/all.xml" ]; then
   find "$PROJECT_DIR" -name pom.xml | grep -v target | xargs dirname | xargs -n1 -IDIR mkdir -p DIR/target/coverage/
   find "$PROJECT_DIR" -name pom.xml | grep -v target | xargs dirname | xargs -n1 -IDIR cp "$PROJECT_DIR/target/coverage/all.xml" DIR/target/coverage/
fi

mvn -B verify -DskipShade -DskipTests -Dskip.npx -Dskip.installnpx org.sonarsource.scanner.maven:sonar-maven-plugin:3.6.0.1398:sonar -Dsonar.host.url=https://sonarcloud.io -Dsonar.organization=apache -Dsonar.projectKey=hadoop-ozone
