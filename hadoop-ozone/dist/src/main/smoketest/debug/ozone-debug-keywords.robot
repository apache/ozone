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

Parse replicas verify JSON output
    [Arguments]    ${output}
    ${json} =      Evaluate    json.loads('''${output}''')    json
    [Return]       ${json}

Check to Verify Replicas
    [Arguments]    ${json}  ${check_type}  ${faulty_datanode}  ${expected_message}
    ${replicas} =    Get From Dictionary    ${json['keys'][0]['blocks'][0]}    replicas
    FOR    ${replica}    IN    @{replicas}
        ${datanode} =     Get From Dictionary    ${replica}    datanode
        ${hostname} =     Get From Dictionary    ${datanode}   hostname
        Run Keyword If    '${hostname}' == '${faulty_datanode}'    Check Replica Failed    ${replica}  ${check_type}  ${expected_message}
        Run Keyword If    '${hostname}' != '${faulty_datanode}'    Check Replica Passed    ${replica}  ${check_type}
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
