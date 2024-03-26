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
Documentation       This test suite verifies the functionality of S3 secure mode in a Ceph cluster environment using docker-compose.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}     http://s3g:9878
${TEMPDIR}          /tmp
${TEST_FILE}        NOTICE.txt


*** Keywords ***
Initialize User Credentials
    [Arguments]     ${username}    ${keytab}
                    Run Keyword        Kinit test user     ${username}    ${keytab}
                    Execute and Ignore error    ozone s3 revokesecret -y -u ${username}
    ${output} =     Execute    ozone s3 getsecret -u ${username}
    ${accessId} =   Get Regexp Matches    ${output}    (?<=awsAccessKey=).*
    ${secretKey} =  Get Regexp Matches    ${output}    (?<=awsSecret=).*
    ${accessId} =   Set Variable         ${accessId[0]}
    ${secretKey} =  Set Variable         ${secretKey[0]}
                    Set Global Variable    ${ACCESS_ID_${username}}    ${accessId}
                    Set Global Variable    ${SECRET_KEY_${username}}   ${secretKey}

Initialize AWS S3
    [Arguments]    ${acess_id}    ${secret_key}
    Execute        aws configure set aws_access_key_id ${acess_id}
    Execute        aws configure set aws_secret_access_key ${secret_key}

*** Test Cases ***

Initialize testuser Credentials
    Initialize User Credentials    testuser    testuser.keytab
    Initialize AWS S3              ${ACCESS_ID_testuser}    ${SECRET_KEY_testuser}

Create Bucket
    ${output} =      Execute    aws s3api --endpoint-url ${ENDPOINT_URL} create-bucket --bucket bucket-test-owner1
    Should contain   ${output}    bucket-test-owner1

Verify Owner of Uploaded S3 Object By testuser
                    Execute    echo "Randomtext" > /tmp/testfile
                    Execute and checkrc    aws s3api --endpoint-url ${ENDPOINT_URL} put-object --bucket bucket-test-owner1 --key mykey --body /tmp/testfile  0
    ${result} =     Execute    ozone sh key info /s3v/bucket-test-owner1/mykey | jq -r '.owner'
                    Should Be Equal    ${result}    testuser