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
    start_end::group_start "Get Rocks Native Git sha"
    echo
    echo "git log -n 1 --format=\"%h\" ./hadoop-hdds/rocks-native"
    ROCKS_NATIVE_GIT_SHA=$(git log -n 1 --format="%h" ./hadoop-hdds/rocks-native)
    echo "ROCKS_NATIVE_GIT_SHA"
    echo
    echo "${ROCKS_NATIVE_GIT_SHA}"
    readonly ROCKS_NATIVE_GIT_SHA
    start_end::group_end
}

function init_native_maven_opts() {
    if [[ "${FORCE_NATIVE_BUILD}" == "true" ]]; then
        NATIVE_MAVEN_OPTIONS="-Drocks_tools_native"
    else
        get_rocks_native_git_sha
        PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
        VERSION_NUMBER=$(echo "${PROJECT_VERSION}"| cut -f1 -d'-')
        EXPECTED_ROCKS_NATIVE_VERSION=${VERSION_NUMBER}".${ROCKS_NATIVE_GIT_SHA}"${PROJECT_VERSION:${#VERSION_NUMBER}}
        echo "Checking Maven repo contains hdds-rocks-native of version ${EXPECTED_ROCKS_NATIVE_VERSION}"
        mvn --non-recursive dependency:get -Dartifact=org.apache.ozone:hdds-rocks-native:${EXPECTED_ROCKS_NATIVE_VERSION}

        EXPECTED_ROCKS_NATIVE_VERSION_EXISTS=$?
        if [[ "${EXPECTED_ROCKS_NATIVE_VERSION_EXISTS}" == "0" ]]; then
          echo "Build using hdds-rocks-native version: ${EXPECTED_ROCKS_NATIVE_VERSION}"
          NATIVE_MAVEN_OPTIONS="-Dhdds.rocks.native.version=${EXPECTED_ROCKS_NATIVE_VERSION}"
        else
          echo "Building hdds-rocks-native module as version ${EXPECTED_ROCKS_NATIVE_VERSION} was not found"
          NATIVE_MAVEN_OPTIONS="-Drocks_tools_native"
        fi
    fi
    echo "Native Maven options : ${NATIVE_MAVEN_OPTIONS}"
}

