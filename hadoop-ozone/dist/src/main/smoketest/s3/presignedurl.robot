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
Library             ./presigned_url_helper.py
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${OZONE_TEST}         true
${BUCKET}             generated

*** Keywords ***
Generate Presigned URL
    [Arguments]    ${bucket}    ${key}    ${expiry}=3600
    ${result} =    Execute    aws s3 presign s3://${bucket}/${key} --endpoint-url ${ENDPOINT_URL} --expires-in ${expiry}
    [Return]    ${result}

*** Test Cases ***
Presigned URL PUT Object
    [Documentation]    Test presigned URL PUT object
    Execute                  echo "Randomtext" > /tmp/testfile
    ${ACCESS_KEY} =          Execute    aws configure get aws_access_key_id
    ${SECRET_ACCESS_KEY} =   Execute    aws configure get aws_secret_access_key
    ${presigned_url}=        Generate Presigned Put Object Url    ${ACCESS_KEY}    ${SECRET_ACCESS_KEY}    ${BUCKET}    test-presigned-put    us-east-1    3600    ${EMPTY}    ${ENDPOINT_URL}
    ${result} =              Execute    curl -X PUT -T "/tmp/testfile" "${presigned_url}"
    Should Not Contain       ${result}    Error
    ${head_result} =         Execute AWSS3ApiCli    head-object --bucket ${BUCKET} --key test-presigned-put
    Should Not Contain       ${head_result}    Error

Presigned URL PUT Object using wrong x-amz-content-sha256
    [Documentation]    Test presigned URL PUT object with wrong x-amz-content-sha256
    Execute                   echo "Randomtext" > /tmp/testfile
    ${presigned_url} =        Generate Presigned URL    ${BUCKET}    test-presigned-put-wrong-sha
    ${result} =               Execute    curl -X PUT -T "/tmp/testfile" -H "x-amz-content-sha256: wronghash" "${presigned_url}"
    Should Contain            ${result}    The provided 'x-amz-content-sha256' header does not match the computed hash.
