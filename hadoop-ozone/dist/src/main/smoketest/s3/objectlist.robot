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
Documentation     S3 max-keys validation test for negative and zero values
Library           OperatingSystem
Library           String
Resource          ../commonlib.robot
Resource          commonawslib.robot
Test Timeout      3 minutes
Suite Setup       Setup s3 tests

*** Variables ***
${ENDPOINT_URL}   http://s3g:9878
${BUCKET}         generated

*** Keywords ***
Prepare Many Objects In Bucket
    [Arguments]    ${count}=1100
    Execute    mkdir -p /tmp/manyfiles
    FOR    ${i}    IN RANGE    ${count}
        Execute    echo "test-${i}" > /tmp/manyfiles/obj-${i}
    END
    Execute    aws s3 cp /tmp/manyfiles s3://${BUCKET}/ --recursive --endpoint-url=${ENDPOINT_URL}

*** Test Cases ***

List objects with negative max-keys should fail
    ${result} =    Execute AWSS3APICli and checkrc    list-objects-v2 --bucket ${BUCKET} --max-keys -1    255
    Should Contain    ${result}    InvalidArgument

List objects with zero max-keys should not fail
    ${result} =    Execute AWSS3APICli and checkrc    list-objects-v2 --bucket ${BUCKET} --max-keys 0    0
    Should not Contain    ${result}    InvalidArgument

List objects with max-keys exceeding config limit should not return more than limit
    Prepare Many Objects In Bucket    1100
    ${result}=    Execute AWSS3APICli and checkrc    list-objects-v2 --bucket ${BUCKET} --max-keys 9999 --endpoint-url=${ENDPOINT_URL} --output json    0
    ${tmpfile}=   Generate Random String    8
    ${tmpfile}=   Set Variable    /tmp/result_${tmpfile}.json
    Create File   ${tmpfile}    ${result}
    ${count}=     Execute and checkrc    jq -r '.Contents | length' ${tmpfile}    0
    Should Be True    ${count} <= 1000
    Remove File   ${tmpfile}

List objects with max-keys less than config limit should return correct count
    Prepare Many Objects In Bucket    1100
    ${result}=    Execute AWSS3APICli and checkrc    list-objects-v2 --bucket ${BUCKET} --max-keys 500 --endpoint-url=${ENDPOINT_URL} --output json    0
    ${tmpfile}=   Generate Random String    8
    ${tmpfile}=   Set Variable    /tmp/result_${tmpfile}.json
    Create File   ${tmpfile}    ${result}
    ${count}=     Execute and checkrc    jq -r '.Contents | length' ${tmpfile}    0
    Should Be True    ${count} == 500
    Remove File   ${tmpfile}

Check Bucket Ownership Verification
    Prepare Many Objects In Bucket                                1
    ${correct_owner} =    Get bucket owner                        ${BUCKET}

    Execute AWSS3APICli with bucket owner check                   list-objects-v2 --bucket ${BUCKET}  ${correct_owner}
