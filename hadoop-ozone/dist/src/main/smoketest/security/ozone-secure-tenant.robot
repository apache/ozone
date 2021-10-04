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
Documentation       Smoke test for ozone secure tenant commands.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${RANGER_ENDPOINT_URL}  https://ranger:6182

*** Keywords ***
Init Ranger MockServer
    ${output} =         Execute          curl -k ${RANGER_ENDPOINT_URL}
                        Should contain   ${output}         {}

*** Test Cases ***
Secure Tenant Create Tenant Success
#    Run Keyword   Kinit test user     testuser     testuser.keytab
    Run Keyword         Init Ranger MockServer
    ${output} =         Execute          ozone tenant create finance
                        Should contain   ${output}         Created tenant 'finance'

Secure Tenant Assign User Success
    ${output} =         Execute          ozone tenant user assign bob@EXAMPLE.COM --tenant=finance
                        Should contain   ${output}         Assigned 'bob@EXAMPLE.COM' to 'finance'

Secure Tenant GetUserInfo Success
    ${output} =         Execute          ozone tenant user info bob@EXAMPLE.COM
                        Should contain   ${output}         Tenant 'finance' with accessId 'finance$bob@EXAMPLE.COM'

Secure Tenant Assign User Failure
    ${rc}  ${result} =  Run And Return Rc And Output  ozone tenant user assign bob@EXAMPLE.COM --tenant=nonexistenttenant
#    Should Be True	${rc} > 0
                        Should contain   ${result}         tenant 'nonexistenttenant' doesn't exist

