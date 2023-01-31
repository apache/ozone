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

*** Settings ***
Documentation       Check Metadata layout version present in a version file.
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${version_prefix}    layoutVersion=

*** Keywords ***
Check version
    ${version_file_contents} =    Get file    ${VERSION_FILE}
    ${version_line} =    Catenate    SEPARATOR=   ${version_prefix}    ${VERSION}
    Should contain    ${version_file_contents}    ${version_line}

*** Test Cases ***
Check MLV
    # Fail if required variables are not set.
    Should not be empty    ${VERSION_FILE}
    Should not be empty    ${VERSION}

    File should exist    ${VERSION_FILE}
    File should not be empty    ${VERSION_FILE}

    Wait until keyword succeeds    3min    10sec    Check version
