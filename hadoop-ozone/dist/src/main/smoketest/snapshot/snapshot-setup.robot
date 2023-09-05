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
Documentation       Test for using fs commands with snapshots.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot

*** Variables ***
${SNAPSHOT_INDICATOR}      .snapshot
${VOLUME}
${BUCKET}

*** Keywords ***
Create volume
    ${random} =     Generate Random String  5  [LOWER]
    ${volume} =     Set Variable        vol-${random}
    ${result} =     Execute             ozone sh volume create /${volume}
                    Should not contain  ${result}       Failed
    [Return]        ${volume}

Create bucket
    [Arguments]     ${volume}           ${bucket_layout}
    ${random} =     Generate Random String  5  [LOWER]
    ${bucket} =     Set Variable        buc-${random}
    ${result} =     Execute             ozone sh bucket create -l ${bucket_layout} /${volume}/${bucket}
                    Should not contain  ${result}       Failed
    [Return]        ${bucket}

Create key
    [Arguments]     ${volume}           ${bucket}       ${file}
    ${random} =     Generate Random String  5  [LOWER]
    ${key} =        Set Variable        key-${random}
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/${key} ${file}
    [Return]        ${key}

Create snapshot
    [Arguments]     ${volume}           ${bucket}
    ${random} =     Generate Random String  5  [LOWER]
    ${snapshot} =   Set Variable        snap-${random}
    ${result} =     Execute             ozone sh snapshot create /${volume}/${bucket} ${snapshot}
                    Should not contain  ${result}       Failed
    [Return]        ${snapshot}

Setup volume and bucket
    ${volume} =             Create Volume
    Set Suite Variable      ${VOLUME}           ${volume}
    ${bucket} =             Create bucket       ${VOLUME}       FILE_SYSTEM_OPTIMIZED
    Set Suite Variable      ${BUCKET}           ${bucket}
