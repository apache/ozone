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
Documentation       Test ozone debug CLI
Library             OperatingSystem
Library             Collections
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute
Suite Setup         Write keys

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-volume${PREFIX}
${BUCKET}           cli-debug-bucket
${BASE_PATH}        o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}
${DEBUGKEY}         debugKey
${TESTFILE}         testfile
${RATIS_ONE_KEY}    ratis-one-key
${RATIS_THREE_KEY}  ratis-three-key
${EC_KEY}           ec-key

*** Keywords ***
Write keys
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME} --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create ${BASE_PATH} --space-quota 1TB
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=100000 count=15
    # Create default key (RATIS THREE by default)
    Execute             ozone sh key put ${BASE_PATH}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}
    # Create RATIS ONE key
    Create Key          ${BASE_PATH}/${RATIS_ONE_KEY}       ${TEMP_DIR}/${TESTFILE}    --type RATIS --replication ONE
    # Create RATIS THREE key
    Create Key          ${BASE_PATH}/${RATIS_THREE_KEY}     ${TEMP_DIR}/${TESTFILE}    --type RATIS --replication THREE
    # Create EC rs-3-2-1024k key
    Create Key          ${BASE_PATH}/${EC_KEY}              ${TEMP_DIR}/${TESTFILE}    --type EC --replication rs-3-2-1024k

Execute and validate replicas verify with filter
    [Arguments]           ${replication_type}    ${replication_factor}    ${verification_type}    ${expected_key_count}
    ${output} =           Execute replicas verify with replication filter    ${replication_type}    ${replication_factor}    ${verification_type}
    ${json} =             Parse replicas verify JSON output      ${output}
    ${keys} =             Get From Dictionary     ${json}         keys
    ${key_count} =        Get Length              ${keys}
    Should Be Equal As Integers           ${key_count}    ${expected_key_count}
    ${key_names} =        Get key names from output    ${json}
    [Return]              ${key_names}

*** Test Cases ***
Test ozone debug replicas verify checksums, block-existence and container-state
    ${output} =          Execute   ozone debug replicas verify --checksums --block-existence --container-state ${BASE_PATH}/${TESTFILE}
    ${json} =            Parse replicas verify JSON output      ${output}

    # 'keys' array should be empty if all keys and their replicas passed
    Should Be Empty      ${json}[keys]
    Should Be True       ${json}[pass]     ${True}

Test ozone debug replicas verify with RATIS ONE filter
    ${key_names} =        Execute and validate replicas verify with filter    RATIS    ONE    checksums    1

    # Should only contain RATIS ONE key
    Should Contain        ${key_names}    ${RATIS_ONE_KEY}      Key ${RATIS_ONE_KEY} not found in output
    # Verify EC and RATIS THREE keys are not present
    Should Not Contain    ${key_names}    ${EC_KEY}             Key ${EC_KEY} should not be in filtered output
    Should Not Contain    ${key_names}    ${TESTFILE}           Key ${TESTFILE} should not be in filtered output
    Should Not Contain    ${key_names}    ${RATIS_THREE_KEY}    Key ${RATIS_THREE_KEY} should not be in filtered output

Test ozone debug replicas verify with RATIS THREE filter
    ${key_names} =        Execute and validate replicas verify with filter    RATIS    THREE    checksums    2

    # Should contain RATIS THREE keys (default testfile and explicit RATIS THREE key)
    Should Contain        ${key_names}    ${TESTFILE}           Key ${TESTFILE} not found in output
    Should Contain        ${key_names}    ${RATIS_THREE_KEY}    Key ${RATIS_THREE_KEY} not found in output
    # Verify RATIS ONE and EC keys are not present
    Should Not Contain    ${key_names}    ${RATIS_ONE_KEY}      Key ${RATIS_ONE_KEY} should not be in filtered output
    Should Not Contain    ${key_names}    ${EC_KEY}             Key ${EC_KEY} should not be in filtered output

Test ozone debug replicas verify with EC rs-3-2-1024k filter
    ${key_names} =        Execute and validate replicas verify with filter    EC    rs-3-2-1024k    checksums    1

    # Should only contain EC key
    Should Contain        ${key_names}    ${EC_KEY}             Key ${EC_KEY} not found in output
    # Verify RATIS keys are not present
    Should Not Contain    ${key_names}    ${TESTFILE}           Key ${TESTFILE} should not be in filtered output
    Should Not Contain    ${key_names}    ${RATIS_ONE_KEY}      Key ${RATIS_ONE_KEY} should not be in filtered output
    Should Not Contain    ${key_names}    ${RATIS_THREE_KEY}    Key ${RATIS_THREE_KEY} should not be in filtered output

Test ozone debug version
    ${output} =    Execute    ozone debug version
                   Execute    echo '${output}' | jq -r '.' # validate JSON
