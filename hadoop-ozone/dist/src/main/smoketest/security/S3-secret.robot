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
Documentation       Smoke test to start cluster with docker-compose environments.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        2 minutes

*** Keywords ***
GetSecret success
    ${output}=             Execute             ozone s3 getsecret -u testuser2
    Should contain         ${output}           awsAccessKey
    Should contain         ${output}           awsSecret
    Should not contain     ${output}           isDeleted
    Should not contain     ${output}           transactionLogIndex

GetSecret failure
    ${output2}=            Execute and Ignore Error    ozone s3 getsecret -u testuser2
    Should not contain     ${output2}           awsAccessKey
    Should not contain     ${output2}           awsSecret
    Should contain         ${output2}           S3_SECRET_ALREADY_EXISTS

Revoke Secrets
    ${output}=       Execute and Ignore Error      ozone s3 revokesecret -y -u testuser2

*** Test Cases ***
Get S3 secret twice
    Run Keyword   Kinit test user     testuser     testuser.keytab
    Revoke Secrets
    GetSecret success
    GetSecret failure
    Revoke Secrets
