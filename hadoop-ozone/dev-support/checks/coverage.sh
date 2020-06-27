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

# This script merges the combined jacoco files (output of unit.sh and others)
# and generates a report in HTML and XML formats

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR/../../.." || exit 1

set -ex

REPORT_DIR="$DIR/../../../target/coverage"

mkdir -p "$REPORT_DIR"

#Install jacoco cli
mvn --non-recursive --no-transfer-progress \
  org.apache.maven.plugins:maven-dependency-plugin:3.1.2:copy \
  -Dartifact=org.jacoco:org.jacoco.cli:0.8.5:jar:nodeps

jacoco() {
  java -jar target/dependency/org.jacoco.cli-0.8.5-nodeps.jar "$@"
}

#Merge all the jacoco.exec files
jacoco merge $(find target -name jacoco-combined.exec) --destfile "$REPORT_DIR/jacoco-all.exec"

rm -rf target/coverage-classes || true
mkdir -p target/coverage-classes

#Unzip all the classes from the last build
find hadoop-ozone/dist/target/*/share/ozone/lib -name "hadoop-*.jar" | \
    grep -E 'hadoop-ozone-|hadoop-hdds-' | \
    grep -v -E 'shaded|hadoop2|hadoop3|tests' | \
    xargs -n1 unzip -o -q -d target/coverage-classes

#Exclude some classes from the coverage
find target/coverage-classes -name proto -type d | xargs rm -rf
find target/coverage-classes -name generated -type d | xargs rm -rf
find target/coverage-classes -name v1 -type d | xargs rm -rf
find target/coverage-classes -name freon -type d | xargs rm -rf
find target/coverage-classes -name genesis -type d | xargs rm -rf

#generate the reports
jacoco report "$REPORT_DIR/jacoco-all.exec" --classfiles target/coverage-classes --html "$REPORT_DIR/all" --xml "$REPORT_DIR/all.xml"
