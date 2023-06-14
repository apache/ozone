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
Create snapshots
    [Tags]     finalized-snapshot-tests
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
    [Tags]     pre-finalized-snapshot-tests
    ${output} =         Execute And Ignore Error    ozone sh volume create snapvolume-2     
                        Should not contain     ${output}       Failed
    ${output} =         Execute And Ignore Error    ozone sh bucket create /snapvolume-2/snapbucket-1     
                        Should not contain     ${output}       Failed
    ${output} =         Execute and checkrc         ozone sh snapshot create /snapvolume-2/snapbucket-1 snapshot1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

List snapshots
    [Tags]     finalized-snapshot-tests
    ${output} =         Execute           ozone sh snapshot ls /snapvolume-1/snapbucket-1
                        Should contain    ${output}       snapshot1
                        Should contain    ${output}       snapshot2
                        Should contain    ${output}       SNAPSHOT_ACTIVE

Attempt to list snapshot when snapshot feature is disabled
    [Tags]     pre-finalized-snapshot-tests
    ${output} =         Execute and checkrc         ozone sh snapshot ls /snapvolume-2/snapbucket-1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

Snapshot Diff
    [Tags]     finalized-snapshot-tests
    WHILE   True
        ${output} =       Execute      ozone sh snapshot snapshotDiff /snapvolume-1/snapbucket-1 snapshot1 snapshot2
        IF                "Snapshot diff job is IN_PROGRESS" in """${output}"""
                          Sleep   10s
        ELSE
                          BREAK
        END
    END
    Should contain    ${output}       +    key1

Attempt to snapshotDiff when snapshot feature is disabled
    [Tags]     pre-finalized-snapshot-tests
    ${output} =         Execute and checkrc         ozone sh snapshot snapshotDiff /snapvolume-2/snapbucket-1 snapshot1 snapshot2    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

Delete snapshot
    [Tags]     finalized-snapshot-tests
    ${output} =         Execute           ozone sh snapshot delete /snapvolume-1/snapbucket-1 snapshot1
                        Should not contain      ${output}       Failed

    ${output} =         Execute            ozone sh snapshot ls /snapvolume-1/snapbucket-1 | jq '[.[] | select(.name == "snapshot1") | .snapshotStatus] | if length > 0 then .[] else "SNAPSHOT_DELETED" end'
                        Should contain   ${output}   SNAPSHOT_DELETED

Attempt to delete when snapshot feature is disabled
    [Tags]     pre-finalized-snapshot-tests
    ${output} =         Execute and checkrc         ozone sh snapshot delete /snapvolume-2/snapbucket-1 snapshot1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION
