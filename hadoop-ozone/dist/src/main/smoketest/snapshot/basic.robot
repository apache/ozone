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
Documentation       Test Snapshot Commands
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../ec/lib.resource
Suite Setup         Prepare For Tests

*** Variables ***
${volume}       snapvolume
${bucket}       snapbucket

*** Test Cases ***
Snapshot Creation
    ${result} =     Execute             ozone sh volume create /${volume}
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${volume}/${bucket}
                    Should not contain  ${result}       Failed
                    Execute             ozone sh key put /${volume}/${bucket}/key1 /tmp/1mb
    ${result} =     Execute             ozone sh snapshot create /${volume}/${bucket} snapshot1
                    Should not contain  ${result}       Failed

Snapshot List

    ${result} =     Execute             ozone sh snapshot ls /${volume}/${bucket}
                    Should contain  ${result}       snapshot1
                    Should contain  ${result}       SNAPSHOT_ACTIVE


Snapshot Diff
                    Execute             ozone sh key put /${volume}/${bucket}/key2 /tmp/2mb
                    Execute             ozone sh key put /${volume}/${bucket}/key3 /tmp/2mb
                    Execute             ozone sh snapshot create /${volume}/${bucket} snapshot2
    ${result} =     Execute             ozone sh snapshot snapshotDiff /${volume}/${bucket} snapshot1 snapshot2
                    Should contain  ${result}       +    key2
                    Should contain  ${result}       +    key3

Read Snapshot
                    Key Should Match Local File         /${volume}/${bucket}/.snapshot/snapshot1/key1      /tmp/1mb
                    Key Should Match Local File         /${volume}/${bucket}/.snapshot/snapshot2/key2      /tmp/2mb
                    Key Should Match Local File         /${volume}/${bucket}/.snapshot/snapshot2/key3      /tmp/2mb

