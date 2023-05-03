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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Create snapshot
    [Tags]     snapshot-support
    ${output} =         Execute                ozone sh volume create vol
                        Should not contain     ${output}       Failed
    ${output} =         Execute                ozone sh bucket create /vol/bucket2
                        Should not contain     ${output}       Failed
    ${output} =         Execute                ozone sh snapshot create /vol/bucket2 snapshot1
                        Should not contain     ${output}       Failed
                        Execute and checkrc    echo "key created using Ozone Shell" > /tmp/sourcekey    0
                        Execute                ozone sh key put /vol/bucket2/key1 /tmp/sourcekey
                        Execute                ozone sh snapshot create /vol/bucket2 snapshot2

Create snapshot in non-supported ozone version
    [Tags]     snapshot-non-support
    ${output} =         Execute and checkrc    ozone sh volume create vol     0
    ${output} =         Execute and checkrc    ozone sh bucket create /vol/bucket2     0
    ${output} =         Execute and checkrc    ozone sh snapshot create /vol/bucket2 snapshot1     255

List snapshot
    [Tags]     snapshot-support
    ${output} =         Execute           ozone sh snapshot ls /vol/bucket2
                        Should contain    ${output}       snapshot1
                        Should contain    ${output}       snapshot2
                        Should contain    ${output}       SNAPSHOT_ACTIVE

List snapshot in non-supported ozone version
    [Tags]     snapshot-non-support
    ${output} =         Execute and checkrc       ozone sh snapshot ls /vol/bucket2    255

Snapshot Diff
    [Tags]     snapshot-support
    ${output} =         Execute           ozone sh snapshot snapshotDiff /vol/bucket2 snapshot1 snapshot2
                        Should contain    ${output}       Snapshot diff job is IN_PROGRESS
    ${output} =         Execute           ozone sh snapshot snapshotDiff /vol/bucket2 snapshot1 snapshot2
                        Should contain    ${output}       +    key1

Snapshot Diff in non-supported ozone version
    [Tags]     snapshot-non-support
    ${output} =         Execute and checkrc          ozone sh snapshot snapshotDiff /vol/bucket2 snapshot1 snapshot2     255

Delete snapshot
    [Tags]     snapshot-support
    ${output} =         Execute           ozone sh snapshot delete /vol/bucket2 snapshot1
                        Should not contain      ${output}       Failed
    ${output} =         Execute           ozone sh snapshot ls /vol/bucket2
                        Should contain          ${output}       SNAPSHOT_DELETED

Delete snapshot in non-supported ozone version
    [Tags]     snapshot-non-support
    ${output} =         Execute and checkrc          ozone sh snapshot delete /vol/bucket2 snapshot1     255

