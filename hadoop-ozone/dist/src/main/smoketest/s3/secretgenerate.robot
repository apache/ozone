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
Documentation       S3 Secret Generate test
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ./commonawslib.robot
Test Timeout        5 minutes
Default Tags        no-bucket-type
Test Setup          Run Keywords       Kinit test user    testuser    testuser.keytab
...                 AND                Revoke S3 secrets
Test Teardown       Run Keyword        Revoke S3 secrets

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${SECURITY_ENABLED}   true

*** Test Cases ***

S3 Gateway Generate Secret
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
    ${result} =         Execute                             curl -X PUT --negotiate -u : -v ${ENDPOINT_URL}/secret
                        Should contain          ${result}       HTTP/1.1 200 OK    ignore_case=True
                        Should Match Regexp     ${result}       <awsAccessKey>.*</awsAccessKey><awsSecret>.*</awsSecret>

S3 Gateway Secret Already Exists
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
                        Execute                             ozone s3 getsecret ${OM_HA_PARAM}
    ${result} =         Execute                             curl -X PUT --negotiate -u : -v ${ENDPOINT_URL}/secret
                        Should contain          ${result}       HTTP/1.1 400 S3_SECRET_ALREADY_EXISTS    ignore_case=True

S3 Gateway Generate Secret By Username
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
    ${result} =         Execute                             curl -X PUT --negotiate -u : -v ${ENDPOINT_URL}/secret/testuser
                        Should contain          ${result}       HTTP/1.1 200 OK    ignore_case=True
                        Should Match Regexp     ${result}       <awsAccessKey>.*</awsAccessKey><awsSecret>.*</awsSecret>

S3 Gateway Generate Secret By Username For Other User
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
    ${result} =         Execute                             curl -X PUT --negotiate -u : -v ${ENDPOINT_URL}/secret/testuser2
                        Should contain          ${result}       HTTP/1.1 200 OK    ignore_case=True
                        Should Match Regexp     ${result}       <awsAccessKey>.*</awsAccessKey><awsSecret>.*</awsSecret>