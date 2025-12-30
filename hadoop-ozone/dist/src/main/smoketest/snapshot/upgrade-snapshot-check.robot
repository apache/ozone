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
Default Tags        pre-finalized-snapshot-tests
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Attempt to create snapshot when snapshot feature is disabled
    ${output} =         Execute And Ignore Error    ozone sh volume create snapvolume-2     
                        Should not contain     ${output}       Failed
    ${output} =         Execute And Ignore Error    ozone sh bucket create /snapvolume-2/snapbucket-1     
                        Should not contain     ${output}       Failed
    ${output} =         Execute and checkrc         ozone sh snapshot create /snapvolume-2/snapbucket-1 snapshot1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

Attempt to list snapshot when snapshot feature is disabled
    ${output} =         Execute and checkrc         ozone sh snapshot ls /snapvolume-2/snapbucket-1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

Attempt to snapshotDiff when snapshot feature is disabled
    ${output} =         Execute and checkrc         ozone sh snapshot snapshotDiff /snapvolume-2/snapbucket-1 snapshot1 snapshot2    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION

Attempt to delete when snapshot feature is disabled
    ${output} =         Execute and checkrc         ozone sh snapshot delete /snapvolume-2/snapbucket-1 snapshot1    255
                        Should contain    ${output}   NOT_SUPPORTED_OPERATION
