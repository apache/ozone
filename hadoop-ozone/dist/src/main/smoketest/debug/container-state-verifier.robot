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
Documentation       Test container state on a UNHEALTHY, DELETED and INVALID container
Resource            ../ozone-fi/BytemanKeywords.robot
Resource            ozone-debug-keywords.robot

*** Variables ***
${PREFIX}               ${EMPTY}
${DATANODE}             ${EMPTY}
${FAULT_INJ_DATANODE}   ${EMPTY}
${VOLUME}               cli-debug-volume${PREFIX}
${BUCKET}               cli-debug-bucket
${TESTFILE}             testfile
${CHECK_TYPE}           containerState
${TEMPLATE_RULE}        /opt/hadoop/share/ozone/byteman/container-state-template.btm

*** Keywords ***
Create Container State Rule
    [Arguments]          ${state}
    ${rule_file} =       Set Variable    /tmp/container-state-${state}.btm
    Execute              sed 's/STATE_PLACEHOLDER/${state}/g' ${TEMPLATE_RULE} > ${rule_file}
    Return From Keyword  ${rule_file}

Verify Container State with Rule
    [Arguments]          ${expected_state}
    ${rule_file} =       Create Container State Rule    ${expected_state}
    Add Byteman Rule     ${FAULT_INJ_DATANODE}    ${rule_file}
    List Byteman Rules   ${FAULT_INJ_DATANODE}

    ${output} =           Execute replicas verify container state debug tool
    ${json} =             Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${DATANODE}  Replica state is ${expected_state}

    Remove Byteman Rule  ${FAULT_INJ_DATANODE}    ${rule_file}

*** Test Cases ***
Verify Container State With Unhealthy Container Replica
    Verify Container State with Rule      UNHEALTHY

Verify Container State With Deleted Container Replica
    Verify Container State with Rule      DELETED

Verify Container State With Invalid Container Replica
    Verify Container State with Rule      INVALID
