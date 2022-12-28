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
Test Timeout        5 minutes

*** Variables ***
${SNAPSHOT_PREFIX}      .snapshot
${VOLUME}
${BUCKET}
${SNAPSHOT_ONE}

*** Keywords ***
Kinit ozone user
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Create volume
    ${random} =     Generate Random String  5  [LOWER]
                    Set Suite Variable     ${VOLUME}    vol-${random}
    ${result} =     Execute             ozone sh volume create /${VOLUME}
                    Should not contain  ${result}       Failed

Create bucket
    ${random} =     Generate Random String  5  [LOWER]
                    Set Suite Variable     ${BUCKET}    buc-${random}
    ${result} =     Execute             ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${VOLUME}/${BUCKET}
                    Should not contain  ${result}       Failed

Create keys
    ${result} =     Execute             ozone sh key put /${VOLUME}/${BUCKET}/key1 README.md
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${VOLUME}/${BUCKET}/dir1/key2 HISTORY.md
                    Should not contain  ${result}       Failed

Create snapshot
    ${random} =     Generate Random String  5  [LOWER]
                    Set Suite Variable     ${SNAPSHOT_ONE}    snap-${random}
    ${result} =     Execute             ozone sh snapshot create /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE}
                    Should not contain  ${result}       Failed

List snapshots with fs -ls
    ${result}       Execute             ozone fs -ls /${VOLUME}/${BUCKET}/${SNAPSHOT_PREFIX}
                    Should contain      ${result}       /${VOLUME}/${BUCKET}/${SNAPSHOT_PREFIX}/${SNAPSHOT_ONE}

Setup Snapshot Paths
    Execute         kdestroy
    Kinit ozone user
    Create volume
    Create bucket
    Create keys

*** Test Cases ***
Test set up paths for the snapshot
    Setup Paths

Test create snapshot
    Create snapshot

Test list snapshots with fs -ls
    List snapshots with fs -ls

