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
Documentation       Test ozone s3 getsecret command
Resource            ../commonlib.robot
Test Timeout        2 minutes
Suite Setup         Kinit test user     testuser     testuser.keytab

*** Keywords ***
Verify output
    [arguments]    ${output}
    Should contain         ${output}           awsAccessKey
    Should contain         ${output}           awsSecret

Revoke secrets
    Execute and Ignore Error             ozone s3 revokesecret -y -u testuser2

*** Test Cases ***
Without OM service ID
    Pass Execution If      '${SECURITY_ENABLED}' == 'false'    N/A
    Revoke secrets
    ${output} =            Execute             ozone s3 getsecret -u testuser2
    Verify output          ${output}
    Revoke secrets

With OM service ID
    Pass Execution If      '${OM_HA_PARAM}' == '${EMPTY}'          duplicate test in non-HA env.
    Pass Execution If      '${SECURITY_ENABLED}' == 'false'    N/A
    Revoke secrets
    ${output} =            Execute             ozone s3 getsecret -u testuser2 ${OM_HA_PARAM}
    Verify output          ${output}
    Revoke secrets
