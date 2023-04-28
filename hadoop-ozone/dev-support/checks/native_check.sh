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

function get_rocks_native_git_sha() {
    echo "Getting Rocks Native Git sha"
    echo
    # Getting the latest git hash for the ./hadoop-hdds/rocks-native directory
    echo "git log -n 1 --format=\"%h\" ./hadoop-hdds/rocks-native"
    ROCKS_NATIVE_GIT_SHA=$(git log -n 1 --format="%h" ./hadoop-hdds/rocks-native)
    echo "ROCKS_NATIVE_GIT_SHA = ${ROCKS_NATIVE_GIT_SHA}"
    readonly ROCKS_NATIVE_GIT_SHA
}

function init_native_maven_opts() {
    get_rocks_native_git_sha
    PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
    # Parsing out version number from project version by getting the first occurance of '-'.
    # If project version is 1.4.0-SNAPSHOT, VERSION_NUMBER = 1.4.0
    VERSION_NUMBER=$(echo "${PROJECT_VERSION}" | cut -f1 -d'-')
    # Adding rocks native sha after the version number in the project version.
    # EXPECTED_ROCK_NATIVE_VERSION = 1.4.0-<rocks native git sha>-SNAPSHOT
    EXPECTED_ROCKS_NATIVE_VERSION=${VERSION_NUMBER}"-${ROCKS_NATIVE_GIT_SHA}"${PROJECT_VERSION:${#VERSION_NUMBER}}
    echo "Checking Maven repo contains hdds-rocks-native of version ${EXPECTED_ROCKS_NATIVE_VERSION}"
    mvn --non-recursive dependency:get -Dartifact=org.apache.ozone:hdds-rocks-native:${EXPECTED_ROCKS_NATIVE_VERSION} -q

    MVN_GET_ROCKS_NATIVE_EXIT_CODE=$?
    if [[ "${MVN_GET_ROCKS_NATIVE_EXIT_CODE}" == "0" ]]; then
      echo "Using existing hdds-rocks-native artifact version: ${EXPECTED_ROCKS_NATIVE_VERSION}"
      NATIVE_MAVEN_OPTIONS="-Dhdds.rocks.native.version=${EXPECTED_ROCKS_NATIVE_VERSION}"
    else
      echo "Building hdds-rocks-native from scratch as version ${EXPECTED_ROCKS_NATIVE_VERSION} was not found in the given Maven repos"
      NATIVE_MAVEN_OPTIONS="-Drocks_tools_native"
    fi
    readonly NATIVE_MAVEN_OPTIONS
    echo "Native Maven options : ${NATIVE_MAVEN_OPTIONS}"
}

