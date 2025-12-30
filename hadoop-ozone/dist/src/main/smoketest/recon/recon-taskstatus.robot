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
Documentation     Test to validate the recon task status API works properly
Library           OperatingSystem
Library           String
Library           BuiltIn
Library           Collections
Resource          ../ozone-lib/freon.robot
Resource          ../commonlib.robot
Test Timeout      5 minutes
Suite Setup         Get Security Enabled From Config

*** Variables ***
${BASE_URL}               http://recon:9888
${TASK_STATUS_ENDPOINT}   ${BASE_URL}/api/v1/task/status
${TRIGGER_SYNC_ENDPOINT}  ${BASE_URL}/api/v1/triggerdbsync/om
${TASK_NAME_1}            ContainerHealthTask
${TASK_NAME_2}            OmDeltaRequest
${BUCKET}                 testbucket
${VOLUME}                 testvolume
${KEYPATH}                ${VOLUME}/${BUCKET}/testkey

*** Keywords ***

Kinit as ozone admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Sync OM Data
  ${result} =             Execute       curl --negotiate -u : -LSs ${TRIGGER_SYNC_ENDPOINT}
  [return]  ${result}

Fetch Task Status
  ${result} =             Execute     curl -H "Accepts: application/json" --negotiate -u : -LSs ${TASK_STATUS_ENDPOINT}
  ${parsed_response} =    Evaluate    json.loads('''${result}''')
  ${tasks} =              Evaluate    [task for task in ${parsed_response}]
  [return]  ${tasks}

*** Test Cases ***

Prepopulate Data and Trigger OM DB Sync
    [Documentation]  Use Freon to prepopulate the OM DB with data and trigger OM DB sync.

    Kinit as ozone admin
    Freon DFSG        n=100        path=${KEYPATH}    size=100

    ${result} =             Sync OM Data
    Should contain          ${result}     true    # Sync should return true if successful

Validate Task Status After Sync
    [Documentation]  Validate that task status is updated after triggering the OM DB sync.

    ${tasks} =    Fetch Task Status
    Should Not Be Empty    ${tasks}

    FOR    ${task}    IN    @{tasks}
        Dictionary Should Contain Key    ${task}    taskName
        Dictionary Should Contain Key    ${task}    lastUpdatedSeqNumber
        Dictionary Should Contain Key    ${task}    lastUpdatedTimestamp
        Dictionary Should Contain Key    ${task}    isCurrentTaskRunning
        Dictionary Should Contain Key    ${task}    lastTaskRunStatus
    END

Validate Stats for Specific Task
    [Documentation]  Validate response for a specific task after OM DB sync.

    ${tasks} =    Fetch Task Status

    ${task_list} =                 Evaluate       [task for task in ${tasks} if task["taskName"] == "${TASK_NAME_1}"]
    ${list_length} =               Get Length     ${task_list}
    Should Be Equal As Integers    ${list_length}    1

    ${task} =    Get From List    ${task_list}    0

    # Validate table fields
    Should Be True        ${task["lastUpdatedTimestamp"]}!=${None}
    Should Be True        ${task["lastUpdatedSeqNumber"]}!=${None}
    Should Be True        ${task["isCurrentTaskRunning"]}!=${None}
    Should Be True        ${task["lastTaskRunStatus"]}!=${None}

Validate All Tasks Updated After Sync
    [Documentation]  Ensure all tasks have been updated after an OM DB sync operation.

    ${tasks} =        Fetch Task Status
    Should Not Be Empty    ${tasks}

    FOR    ${task}    IN    @{tasks}
        Should Be True      ${task["lastUpdatedTimestamp"]}!=${None}
        Should Be True      ${task["lastUpdatedSeqNumber"]}!=${None}
    END

Validate Sequence number is updated after sync
    Sleep  2s               # Waits for 2 seconds for any previous om sync to complete
    Sync OM Data
    ${tasks} =              Fetch Task Status
    Should Not Be Empty     ${tasks}

    ${om_delta_task_list} =        Evaluate       [task for task in ${tasks} if task["taskName"] == "OmDeltaRequest"]
    ${list_length} =               Get Length     ${om_delta_task_list}
    Should Be Equal As Integers    ${list_length}    1

    ${om_delta_task} =              Get From List    ${om_delta_task_list}    0
    ${om_delta_task_seq_num} =      Evaluate      int(${om_delta_task["lastUpdatedSeqNumber"]})
    ${om_task_names} =              Evaluate      ["NSSummaryTask", "ContainerKeyMapperTask", "FileSizeCountTask", "OmTableInsightTask"]
    ${om_tasks} =                   Evaluate      [task for task in ${tasks} if task["taskName"] in ${om_task_names}]

    FOR    ${task}    IN    @{om_tasks}
        IF    ${task["isCurrentTaskRunning"]} == 0
          Should Be Equal As Integers       ${task["lastUpdatedSeqNumber"]}    ${om_delta_task_seq_num}
        END
    END
