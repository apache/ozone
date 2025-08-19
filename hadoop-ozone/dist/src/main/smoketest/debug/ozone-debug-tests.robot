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
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute
Suite Setup         Write keys

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-volume${PREFIX}
${BUCKET}           cli-debug-bucket
${DEBUGKEY}         debugKey
${TESTFILE}         testfile
${RATIS_ONE_KEY}    ratis-one-key
${RATIS_THREE_KEY}  ratis-three-key
${EC_KEY}           ec-key

*** Keywords ***
Write keys
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME} --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} --space-quota 1TB
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=100000 count=15
    # Create default key (RATIS THREE by default)
    Execute             ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}
    # Create RATIS ONE key
    Create test key with replication config    ${RATIS_ONE_KEY}       RATIS    ONE
    # Create RATIS THREE key
    Create test key with replication config    ${RATIS_THREE_KEY}     RATIS    THREE
    # Create EC rs-3-2-1024k key
    Create test key with replication config    ${EC_KEY}              EC        rs-3-2-1024k

*** Test Cases ***
Test ozone debug replicas verify checksums, block-existence and container-state
    ${output} =    Execute   ozone debug replicas verify --checksums --block-existence --container-state o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE}
    ${json} =      Parse replicas verify JSON output      ${output}

    # 'keys' array should be empty if all keys and their replicas passed
    Should Be Empty      ${json}[keys]
    Should Be True       ${json}[pass]     ${True}

Test ozone debug replicas verify with RATIS ONE filter
    ${output} =    Execute replicas verify with replication filter    RATIS    ONE    checksums
    ${json} =      Parse replicas verify JSON output      ${output}

    ${keys} =             Get From Dictionary     ${json}         keys
    ${key_count} =        Get Length              ${keys}
    Should Be Equal As Integers           ${key_count}    1
    # Should only contain RATIS ONE key
    Verify key exists in output           ${json}         ${RATIS_ONE_KEY}
    # Verify EC and RATIS THREE keys are not present
    Verify key not in output              ${json}         ${EC_KEY}
    Verify key not in output              ${json}         ${TESTFILE}
    Verify key not in output              ${json}         ${RATIS_THREE_KEY}

Test ozone debug replicas verify with RATIS THREE filter
    ${output} =    Execute replicas verify with replication filter    RATIS    THREE    checksums
    ${json} =      Parse replicas verify JSON output      ${output}
    
    ${keys} =             Get From Dictionary     ${json}         keys
    ${key_count} =        Get Length              ${keys}
    Should Be Equal As Integers           ${key_count}    2
    # Should contain RATIS THREE keys (default testfile and explicit RATIS THREE key)
    Verify key exists in output           ${json}         ${TESTFILE}
    Verify key exists in output           ${json}         ${RATIS_THREE_KEY}
    # Verify RATIS ONE and EC keys are not present
    Verify key not in output              ${json}         ${RATIS_ONE_KEY}
    Verify key not in output              ${json}         ${EC_KEY}

Test ozone debug replicas verify with EC rs-3-2-1024k filter
    ${output} =    Execute replicas verify with replication filter    EC    rs-3-2-1024k    checksums
    ${json} =      Parse replicas verify JSON output      ${output}

    ${keys} =             Get From Dictionary     ${json}           keys
    ${key_count} =        Get Length              ${keys}
    Should Be Equal As Integers           ${key_count}      1
    # Should only contain EC key
    Verify key exists in output           ${json}           ${EC_KEY}
    # Verify RATIS keys are not present
    Verify key not in output              ${json}           ${TESTFILE}
    Verify key not in output              ${json}           ${RATIS_ONE_KEY}
    Verify key not in output              ${json}           ${RATIS_THREE_KEY}

Test ozone debug version
    ${output} =    Execute    ozone debug version
                   Execute    echo '${output}' | jq -r '.' # validate JSON
