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
Documentation       Smoketest ozone cluster snapshot feature
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Create snapshot
    [Tags]     snapshot-enabled
    ${output} =         Execute                ozone sh volume create snapvolume-1
                        Should not contain     ${output}       Failed
    ${output} =         Execute                ozone sh bucket create /snapvolume-1/snapbucket-1
                        Should not contain     ${output}       Failed
    ${output} =         Execute                ozone sh snapshot create /snapvolume-1/snapbucket-1 snapshot1
                        Should not contain     ${output}       Failed
                        Execute and checkrc    echo "key created using Ozone Shell" > /tmp/sourcekey    0
                        Execute                ozone sh key put /snapvolume-1/snapbucket-1/key1 /tmp/sourcekey
                        Execute                ozone sh snapshot create /snapvolume-1/snapbucket-1 snapshot2

Attempt to create snapshot when snapshot feature is disabled
    [Tags]     snapshot-disabled
    ${output} =         Execute and checkrc    ozone sh volume create snapvolume-2     0
    ${output} =         Execute and checkrc    ozone sh bucket create /snapvolume-2/snapbucket-1     0
    ${output} =         Execute and checkrc    ozone sh snapshot create /snapvolume-2/snapbucket-1 snapshot1     255

List snapshot
    [Tags]     snapshot-enabled
    ${output} =         Execute           ozone sh snapshot ls /snapvolume-1/snapbucket-1
                        Should contain    ${output}       snapshot1
                        Should contain    ${output}       snapshot2
                        Should contain    ${output}       SNAPSHOT_ACTIVE

Attempt to list snapshot when snapshot feature is disabled
    [Tags]     snapshot-disabled
    ${output} =         Execute and checkrc       ozone sh snapshot ls /snapvolume-2/snapbucket-1    255

Snapshot Diff
    [Tags]     snapshot-enabled
    ${output} =         Execute           ozone sh snapshot snapshotDiff /snapvolume-1/snapbucket-1 snapshot1 snapshot2
                        Should contain    ${output}       Snapshot diff job is IN_PROGRESS
    ${output} =         Execute           ozone sh snapshot snapshotDiff /snapvolume-1/snapbucket-1 snapshot1 snapshot2
                        Should contain    ${output}       +    key1

Attempt to snapshotDiff when snapshot feature is disabled
    [Tags]     snapshot-disabled
    ${output} =         Execute and checkrc          ozone sh snapshot snapshotDiff /snapvolume-2/snapbucket-1 snapshot1 snapshot2     255

Delete snapshot
    [Tags]     snapshot-enabled
    ${output} =         Execute           ozone sh snapshot delete /snapvolume-1/snapbucket-1 snapshot1
                        Should not contain      ${output}       Failed
    ${output} =         Execute           ozone sh snapshot ls /snapvolume-1/snapbucket-1
                        Should contain          ${output}       SNAPSHOT_DELETED

Attempt to delete when snapshot feature is disabled
    [Tags]     snapshot-disabled
    ${output} =         Execute and checkrc          ozone sh snapshot delete /snapvolume-2/snapbucket-1 snapshot1     255

