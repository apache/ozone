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
Documentation       Test ozone repair om update-transaction against an offline OM DB checkpoint.
Library             String
Resource            ../lib/os.robot
Test Timeout        5 minutes
Test Setup          Create OM DB Checkpoint
Test Teardown       Remove Directory    ${TEST_DIR}    recursive=True

*** Variables ***
${OM_DB}            /data/metadata/om.db

*** Keywords ***
Create OM DB Checkpoint
    ${test_dir} =    Execute    mktemp -d
    Set Test Variable    ${TEST_DIR}    ${test_dir}
    Set Test Variable    ${DB}    ${TEST_DIR}/om.db
    Execute    ozone debug ldb --db=${OM_DB} checkpoint --output=${DB}

Read Transaction Info
    ${output} =    Execute    ozone debug ldb --db=${DB} scan --cf=transactionInfoTable
    ${info} =      Execute    echo '${output}' | jq -er '.["#TRANSACTIONINFO"].transactionInfoString'
    [return]    ${info}

*** Test Cases ***
Update OM Transaction Info
    ${original} =    Read Transaction Info
    ${term}    ${index} =    Split String    ${original}    \#
    ${term} =     Evaluate    int($term) + 1
    ${index} =    Evaluate    int($index) + 100

    ${output} =    Execute
    ...    ozone repair om update-transaction --db=${DB} --term=${term} --index=${index} --dry-run
    Should Contain    ${output}    [dry run] Updating transaction info to (t:${term}, i:${index})
    ${actual} =    Read Transaction Info
    Should Be Equal    ${actual}    ${original}

    ${output} =    Execute
    ...    echo y | ozone repair om update-transaction --db=${DB} --term=${term} --index=${index}
    Should Contain    ${output}    The highest transaction info has been updated to: (t:${term}, i:${index})
    ${actual} =    Read Transaction Info
    Should Be Equal    ${actual}    ${term}#${index}
