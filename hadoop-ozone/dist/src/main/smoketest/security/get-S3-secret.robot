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
    Run Keyword   Kinit test user     testuser     testuser.keytab
    ${output}=             Execute             ozone s3 getsecret
    Should contain         ${output}           awsAccessKey=
    Should contain         ${output}           awsSecret=

GetSecret failure
    Run Keyword   Kinit test user     testuser     testuser.keytab
    ${output}=             Execute             ozone s3 getsecret
    Should not contain     ${output}           awsAccessKey
    Should not contain     ${output}           awsSecret
    Should contain         ${output}           S3_SECRET_ALREADY_EXISTS

*** Test Cases ***
Get S3 secret twice
    Run Keyword            GetSecret success
    Run Keyword            GetSecret failure
