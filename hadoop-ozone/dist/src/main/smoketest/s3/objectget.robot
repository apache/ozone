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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${OZONE_TEST}         true
${BUCKET}             generated
${EMPTY_FILE}         /tmp/zerobyte
${TEST_FILE}          /tmp/testfile
${TEST_KEY_IN_LINK}   undefined
${TEST_KEY_IN_REAL}   undefined
${EMPTY_KEY_IN_LINK}  undefined
${EMPTY_KEY_IN_REAL}  undefined

*** Keywords ***
Setup tests
    Setup s3 tests
    Create test keys

Create test keys
                              Execute            echo -n > ${EMPTY_FILE}
    ${EMPTY_KEY_IN_LINK} =    Create test key    ${LINKED_VOLUME}    ${LINKED_BUCKET}    empty    ${EMPTY_FILE}
    ${EMPTY_KEY_IN_REAL} =    Create test key    ${VOLUME}           ${BUCKET}           empty    ${EMPTY_FILE}

                              Execute            echo "Randomtext" > ${TEST_FILE}
    ${TEST_KEY_IN_LINK} =     Create test key    ${LINKED_VOLUME}    ${LINKED_BUCKET}    non-empty    ${TEST_FILE}
    ${TEST_KEY_IN_REAL} =     Create test key    ${VOLUME}           ${BUCKET}           non-empty    ${TEST_FILE}

    Set Suite Variable        ${EMPTY_KEY_IN_LINK}
    Set Suite Variable        ${EMPTY_KEY_IN_REAL}
    Set Suite Variable        ${TEST_KEY_IN_LINK}
    Set Suite Variable        ${TEST_KEY_IN_REAL}

Get object from s3
    [Arguments]         ${bucket}    ${key}

    ${result} =         Execute AWSS3ApiCli         get-object --bucket ${bucket} --key ${key} ${TEST_FILE}.result
    Compare files       ${TEST_FILE}                ${TEST_FILE}.result

Get Partial object from s3 with both start and endoffset
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=0-4 ${TEST_FILE}1.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=0 bs=1 count=5 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}1.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=2-4 ${TEST_FILE}1.result1
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 2-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=2 bs=1 count=3 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}1.result1
                                Should Be Equal            ${expectedData}            ${actualData}

# end offset greater than file size and start with in file length
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=2-1000 ${TEST_FILE}1.result2
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 2-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=2 bs=1 count=9 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}1.result2
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3APICli and checkrc        get-object --bucket ${bucket} --key ${key} --range bytes=10000-10000 ${TEST_FILE}2.result   255
                                Should contain             ${result}        InvalidRange


Get Partial object from s3 with both start and endoffset(end offset is greater than file size)
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=0-10000 ${TEST_FILE}2.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat ${TEST_FILE}
    ${actualData} =             Execute                    cat ${TEST_FILE}2.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with only start offset
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=0- ${TEST_FILE}3.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat ${TEST_FILE}
    ${actualData} =             Execute                    cat ${TEST_FILE}3.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 with both start and endoffset which are equal
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=0-0 ${TEST_FILE}4.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-0/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=0 bs=1 count=1 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}4.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=4-4 ${TEST_FILE}5.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 4-4/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=4 bs=1 count=1 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}5.result
                                Should Be Equal            ${expectedData}            ${actualData}

Get Partial object from s3 to get last n bytes
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=-4 ${TEST_FILE}6.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 7-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    dd if=${TEST_FILE} skip=7 bs=1 count=4 2>/dev/null
    ${actualData} =             Execute                    cat ${TEST_FILE}6.result
                                Should Be Equal            ${expectedData}            ${actualData}

# if end is greater than file length, returns whole file
    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=-10000 ${TEST_FILE}7.result
                                Should contain             ${result}        ContentRange
                                Should contain             ${result}        bytes 0-10/11
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat ${TEST_FILE}
    ${actualData} =             Execute                    cat ${TEST_FILE}7.result
                                Should Be Equal            ${expectedData}            ${actualData}

Incorrect values for end and start offset
    [Arguments]         ${bucket}    ${key}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=-11-10000 ${TEST_FILE}8.result
                                Should not contain         ${result}        ContentRange
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat ${TEST_FILE}
    ${actualData} =             Execute                    cat ${TEST_FILE}8.result
                                Should Be Equal            ${expectedData}            ${actualData}

    ${result} =                 Execute AWSS3ApiCli        get-object --bucket ${bucket} --key ${key} --range bytes=11-8 ${TEST_FILE}9.result
                                Should not contain         ${result}        ContentRange
                                Should contain             ${result}        AcceptRanges
    ${expectedData} =           Execute                    cat ${TEST_FILE}
    ${actualData} =             Execute                    cat ${TEST_FILE}8.result
                                Should Be Equal            ${expectedData}            ${actualData}

Zero byte file
    [Arguments]         ${bucket}    ${key}

    ${result} =         Execute AWSS3APICli and checkrc        get-object --bucket ${bucket} --key ${key} --range bytes=0-0 /dev/null    255
                        Should contain             ${result}        InvalidRange

    ${result} =         Execute AWSS3APICli and checkrc        get-object --bucket ${bucket} --key ${key} --range bytes=0-1 /dev/null    255
                        Should contain             ${result}        InvalidRange

    ${result} =         Execute AWSS3APICli and checkrc        get-object --bucket ${bucket} --key ${key} --range bytes=0-10000 /dev/null    255
                        Should contain             ${result}        InvalidRange


# execute all tests twice: with real bucket and with link to bucket in another volume
*** Test Cases ***
Get object from s3
    [Template]    Get object from s3
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 with both start and endoffset
    [Template]    Get Partial object from s3 with both start and endoffset
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)
    [Template]    Get Partial object from s3 with both start and endoffset(start offset and endoffset is greater than file size)
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 with both start and endoffset(end offset is greater than file size)
    [Template]    Get Partial object from s3 with both start and endoffset(end offset is greater than file size)
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 with only start offset
    [Template]    Get Partial object from s3 with only start offset
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 with both start and endoffset which are equal
    [Template]    Get Partial object from s3 with both start and endoffset which are equal
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Get Partial object from s3 to get last n bytes
    [Template]    Get Partial object from s3 to get last n bytes
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Incorrect values for end and start offset
    [Template]    Incorrect values for end and start offset
    ${BUCKET}         ${TEST_KEY_IN_REAL}
    ${BUCKET_LINK}    ${TEST_KEY_IN_LINK}

Zero byte file
    [Template]    Zero byte file
    ${BUCKET}         ${EMPTY_KEY_IN_REAL}
    ${BUCKET_LINK}    ${EMPTY_KEY_IN_LINK}

