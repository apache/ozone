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
Resource            snapshot-setup.robot
Test Timeout        5 minutes

*** Variables ***
${KEY_ONE}
${KEY_TWO}
${SNAPSHOT_ONE}

*** Keywords ***
Keys creation
    ${key_one} =            Create key          ${VOLUME}       ${BUCKET}       /etc/hosts
    Set Suite Variable      ${KEY_ONE}          ${key_one}
    ${key_two} =            Create key          ${VOLUME}       ${BUCKET}       /etc/os-release
    Set Suite Variable      ${KEY_TWO}          ${key_two}

Snapshot creation
    ${snapshot_one} =       Create snapshot     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_ONE}     ${snapshot_one}

List snapshots with fs -ls
    ${result} =     Execute                 ozone fs -ls /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}
                    Should contain          ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}

List snapshot keys with fs -ls
    ${result} =     Execute                 ozone fs -ls /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}
                    Should contain          ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}
                    Should contain          ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_TWO}
    ${result} =     Execute                 ozone fs -ls /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}
                    Should contain          ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}
                    Should not contain      ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_TWO}

*** Test Cases ***
Test set up paths and snapshot
    Setup volume and bucket
    Keys creation
    Snapshot creation

Test list snapshots with fs -ls
    List snapshots with fs -ls

Test list snapshot keys with fs -ls
    List snapshot keys with fs -ls
