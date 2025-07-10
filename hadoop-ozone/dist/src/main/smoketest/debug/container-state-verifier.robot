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
Suite Setup         Write Test Keys

*** Variables ***
${VOLUME}               test-container-vol
${BUCKET}               test-container-buck
${TESTFILE}             test-container-key
${CHECK_TYPE}           containerState
${FAULT_INJ_DATANODE}   datanode1
${FAULTY_DATANODE}      ozonesecure-ha-datanode1-1.ozonesecure-ha_ozone_net

${UNHEALTHY_RULE}       /opt/hadoop/share/ozone/byteman/unhealthy-container-state.btm
${DELETED_RULE}         /opt/hadoop/share/ozone/byteman/deleted-container-state.btm
${INVALID_RULE}         /opt/hadoop/share/ozone/byteman/invalid-container-state.btm

*** Keywords ***
Write Test Keys
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Execute             ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}
    Execute             dd if=/dev/urandom of=/tmp/${TESTFILE} bs=100MB count=1
    Execute             ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} /tmp/${TESTFILE}

*** Test Cases ***
Verify Container State With Unhealthy Container Replica
    Add Byteman Rule      ${FAULT_INJ_DATANODE}    ${UNHEALTHY_RULE}
    List Byteman Rules    ${FAULT_INJ_DATANODE}

    ${output} =           Execute replicas verify container state debug tool
    ${json} =             Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${FAULTY_DATANODE}  Replica state is UNHEALTHY
    
    Remove Byteman Rule   ${FAULT_INJ_DATANODE}    ${UNHEALTHY_RULE}

Verify Container State With Deleted Container Replica
    Add Byteman Rule      ${FAULT_INJ_DATANODE}    ${DELETED_RULE}
    List Byteman Rules    ${FAULT_INJ_DATANODE}

    ${output} =           Execute replicas verify container state debug tool
    ${json} =             Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${FAULTY_DATANODE}  Replica state is DELETED

    Remove Byteman Rule   ${FAULT_INJ_DATANODE}    ${DELETED_RULE}

Verify Container State With Invalid Container Replica
    Add Byteman Rule      ${FAULT_INJ_DATANODE}    ${INVALID_RULE}
    List Byteman Rules    ${FAULT_INJ_DATANODE}

    ${output} =           Execute replicas verify container state debug tool
    ${json} =             Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${FAULTY_DATANODE}  Replica state is INVALID

    Remove Byteman Rule   ${FAULT_INJ_DATANODE}    ${INVALID_RULE}
