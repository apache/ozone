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
Documentation       S3 Conditional Put (If-None-Match / If-Match) tests
Library             OperatingSystem
Library             String
Library             Process
Resource            ../commonlib.robot
Resource            ./commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Conditional Put If-None-Match Star Creates New Key
    [Documentation]    If-None-Match: * should succeed when key does not exist
    ${key} =           Set Variable    condput-ifnonematch-new
                       Execute         echo "test-content" > /tmp/${key}
    ${result} =        Execute AWSS3APICli    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key} --if-none-match *
                       Should contain    ${result}    ETag

Conditional Put If-None-Match Star Fails For Existing Key
    [Documentation]    If-None-Match: * should fail with 412 when key already exists
    ${key} =           Set Variable    condput-ifnonematch-existing
                       Execute         echo "initial-content" > /tmp/${key}
    ${result} =        Execute AWSS3APICli    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key}
                       Should contain    ${result}    ETag
    # Now try again with If-None-Match: *
    ${result} =        Execute AWSS3APICli and ignore error    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key} --if-none-match *
                       Should contain    ${result}    PreconditionFailed

Conditional Put If-Match With Correct ETag Succeeds
    [Documentation]    If-Match with correct ETag should succeed
    ${key} =           Set Variable    condput-ifmatch-success
                       Execute         echo "initial-content" > /tmp/${key}
    ${result} =        Execute AWSS3APICli    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key}
                       Should contain    ${result}    ETag
    # Extract the ETag value
    ${etag} =          Execute And Ignore Error    echo '${result}' | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['ETag'])"
    ${etag} =          Get From List    ${etag}    1
    # Rewrite with matching ETag
                       Execute         echo "updated-content" > /tmp/${key}-updated
    ${result} =        Execute AWSS3APICli    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key}-updated --if-match ${etag}
                       Should contain    ${result}    ETag

Conditional Put If-Match With Wrong ETag Fails
    [Documentation]    If-Match with wrong ETag should fail with 412
    ${key} =           Set Variable    condput-ifmatch-fail
                       Execute         echo "initial-content" > /tmp/${key}
    ${result} =        Execute AWSS3APICli    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key}
                       Should contain    ${result}    ETag
    # Try to rewrite with a wrong ETag
    ${result} =        Execute AWSS3APICli and ignore error    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key} --if-match "wrong-etag"
                       Should contain    ${result}    PreconditionFailed

Conditional Put If-Match On Non-Existent Key Fails
    [Documentation]    If-Match on a key that does not exist should fail with 412
    ${key} =           Set Variable    condput-ifmatch-nonexistent
                       Execute         echo "test-content" > /tmp/${key}
    ${result} =        Execute AWSS3APICli and ignore error    put-object --bucket ${BUCKET} --key ${key} --body /tmp/${key} --if-match "some-etag"
                       Should contain    ${result}    PreconditionFailed
