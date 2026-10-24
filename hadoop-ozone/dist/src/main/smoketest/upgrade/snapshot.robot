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
Documentation       Create a snapshot, read earlier snapshots and diff against the previous one
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***
${PREFIX}
${SNAPSHOT}
${FROM_SNAPSHOT}
${EARLIER_SNAPSHOTS}

*** Keywords ***
Snapshot diff should be done
    ${output} =         Execute          ozone sh snapshot diff --get-report --json /${PREFIX}-volume/${PREFIX}-bucket ${FROM_SNAPSHOT} ${SNAPSHOT} | jq -r '.jobStatus'
                        Should contain   ${output}    DONE

*** Test Cases ***
Create snapshot
    [Tags]    create-snapshot
    ${output} =         Execute          ozone sh snapshot create /${PREFIX}-volume/${PREFIX}-bucket ${SNAPSHOT}
                        Should not contain  ${output}       Failed

Read key from earlier snapshots
    @{snapshots} =      Split String     ${EARLIER_SNAPSHOTS}    ,
    FOR    ${snapshot}    IN    @{snapshots}
        ${output} =     Execute          ozone sh key cat /${PREFIX}-volume/${PREFIX}-bucket/.snapshot/${snapshot}/${PREFIX}-key
                        Should contain   ${output}    ${PREFIX}: key created using Ozone Shell
    END

Diff with previous snapshot
    ${output} =         Execute          ozone sh snapshot diff /${PREFIX}-volume/${PREFIX}-bucket ${FROM_SNAPSHOT} ${SNAPSHOT}
                        Should not contain  ${output}       Failed
    Wait Until Keyword Succeeds    2min    5sec    Snapshot diff should be done
