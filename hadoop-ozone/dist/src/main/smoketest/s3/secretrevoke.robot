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
Documentation       S3 Secret Revoke test
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ./commonawslib.robot
Test Timeout        5 minutes
Default Tags        no-bucket-type
Test Setup          Run Keywords       Kinit test user    testuser    testuser.keytab
...                 AND                Revoke S3 secrets
Suite Setup         Get Security Enabled From Config
Suite Teardown      Setup v4 headers

*** Variables ***
${ENDPOINT_URL}       http://s3g:19878

*** Test Cases ***

S3 Gateway Revoke Secret
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
                        Execute                             ozone s3 getsecret ${OM_HA_PARAM}
    ${result} =         Execute                             curl -X DELETE --negotiate -u : -v ${ENDPOINT_URL}/secret
                        Should contain      ${result}       HTTP/1.1 200 OK    ignore_case=True

S3 Gateway Revoke Secret By Username
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
                        Execute                             ozone s3 getsecret -u testuser ${OM_HA_PARAM}
    ${result} =         Execute                             curl -X DELETE --negotiate -u : -v ${ENDPOINT_URL}/secret/testuser
                        Should contain      ${result}       HTTP/1.1 200 OK    ignore_case=True

S3 Gateway Revoke Secret By Username For Other User
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
                        Execute                             ozone s3 getsecret -u testuser2 ${OM_HA_PARAM}
    ${result} =         Execute                             curl -X DELETE --negotiate -u : -v ${ENDPOINT_URL}/secret/testuser2
                        Should contain      ${result}       HTTP/1.1 200 OK    ignore_case=True

S3 Gateway Reject Secret Revoke By Non-admin User
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
    Run Keyword                                             Kinit test user   testuser2   testuser2.keytab
    ${result} =         Execute                             curl -X DELETE --negotiate -u : -v ${ENDPOINT_URL}/secret/testuser
                        Should contain          ${result}   HTTP/1.1 403 FORBIDDEN    ignore_case=True
