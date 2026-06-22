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
Documentation       Keyword definitions for Ozone Debug CLI tests
Library             Collections
Resource            ../lib/os.robot

*** Variables ***
${OM_SERVICE_ID}                    %{OM_SERVICE_ID}

*** Keywords ***
Execute replicas verify checksums debug tool
    ${output}      Execute          ozone debug replicas verify --checksums o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} --all-results
    [Return]       ${output}

Execute replicas verify block existence debug tool
    ${output}      Execute          ozone debug replicas verify --block-existence o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} --all-results
    [Return]       ${output}

Execute replicas verify container state debug tool
    ${output}      Execute          ozone debug replicas verify --container-state o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} --all-results
    [Return]       ${output}

Parse replicas verify JSON output
    [Arguments]    ${output}
    ${json_split} =  Evaluate  '''${output}'''.split('***')[0].strip()
    ${json} =      Evaluate  json.loads('''${json_split}''')  json
    [Return]       ${json}

Check to Verify Replicas
    [Arguments]    ${json}  ${check_type}  ${faulty_datanode}  ${expected_message}
    ${replicas} =    Get From Dictionary    ${json['keys'][0]['blocks'][0]}    replicas
    Run Keyword If    '${check_type}' == 'containerState'    Check Container State Replicas    ${replicas}  ${faulty_datanode}  ${expected_message}
    ...    ELSE    Check Standard Replicas    ${replicas}  ${check_type}  ${faulty_datanode}  ${expected_message}

Check Standard Replicas
    [Arguments]    ${replicas}  ${check_type}  ${faulty_datanode}  ${expected_message}
    FOR    ${replica}    IN    @{replicas}
        ${datanode} =     Get From Dictionary    ${replica}    datanode
        ${hostname} =     Get From Dictionary    ${datanode}   hostname
        Run Keyword If    '${hostname}' == '${faulty_datanode}'    Check Replica Failed    ${replica}  ${check_type}  ${expected_message}
        Run Keyword If    '${hostname}' != '${faulty_datanode}'    Check Replica Passed    ${replica}  ${check_type}
    END

Check Container State Replicas
    [Arguments]    ${replicas}  ${faulty_datanode}  ${expected_message}
    FOR    ${replica}    IN    @{replicas}
        ${datanode} =     Get From Dictionary    ${replica}    datanode
        ${hostname} =     Get From Dictionary    ${datanode}   hostname
        ${checks} =       Get From Dictionary    ${replica}    checks
        ${check} =        Get From List          ${checks}     0
        Should Be Equal    ${check['type']}    containerState
        Should Be Equal    ${check['pass']}    ${False}
        ${actual_message} =    Set Variable    ${check['failures'][0]['message']}

        Run Keyword If    '${hostname}' == '${faulty_datanode}'    Should Contain    ${actual_message}    ${expected_message}
        ...    ELSE    Should Match Regexp    ${actual_message}    Replica state is (OPEN|CLOSING|QUASI_CLOSED|CLOSED)
    END

Check Replica Failed
    [Arguments]    ${replica}  ${check_type}  ${expected_message}
    ${checks} =     Get From Dictionary    ${replica}    checks
    ${check} =      Get From List          ${checks}     0
    Should Be Equal    ${check['type']}    ${check_type}
    Should Be Equal    ${check['pass']}    ${False}
    Should Contain     ${check['failures'][0]['message']}    ${expected_message}

Check Replica Passed
    [Arguments]    ${replica}    ${check_type}
    ${checks} =    Get From Dictionary    ${replica}    checks
    ${check} =     Get From List          ${checks}     0
    Should Be Equal   ${check['type']}    ${check_type}
    Should Be True    ${check['completed']}
    Should Be True    ${check['pass']}
    Should Be Empty   ${check['failures']}

Execute replicas verify with replication filter
    [Arguments]    ${replication_type}    ${replication_factor}    ${verification_type}
    ${output}      Execute          ozone debug replicas verify --${verification_type} --type ${replication_type} --replication ${replication_factor} o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} --all-results
    [Return]       ${output}

Get key names from output
    [Arguments]    ${json}
    ${keys} =      Get From Dictionary    ${json}    keys
    ${key_names} =    Create List
    FOR    ${key}    IN    @{keys}
        ${key_name} =    Get From Dictionary    ${key}    name
        Append To List    ${key_names}    ${key_name}
    END
    [Return]       ${key_names}

Get chunk-info block sizes by group
    ${output} =    Execute          ozone debug replicas chunk-info o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} | jq -c '[.keyLocations[] | [.[] | {i: .replicaIndex, s: .blockData.size}] | sort_by(.i) | map(.s)]'
    [Return]       ${output}

Verify chunk-info block sizes
    [Arguments]    ${expected_json}
    ${actual_json} =    Get chunk-info block sizes by group
    ${actual} =    Evaluate    json.dumps(json.loads('''${actual_json}'''.strip()))    json, json
    ${expected} =    Evaluate    json.dumps(json.loads('''${expected_json}'''))    json, json
    Should Be Equal As Strings    ${actual}    ${expected}

Create EC key
    [Arguments]    ${ec_data}    ${ec_parity}    ${file_size}
    Execute    dd if=/dev/urandom of=${TEMP_DIR}/testfile bs=1 count=${file_size}
    Execute    ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/testfile ${TEMP_DIR}/testfile -r rs-${ec_data}-${ec_parity}-1024k -t EC

Create Volume Bucket
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Execute             ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}
