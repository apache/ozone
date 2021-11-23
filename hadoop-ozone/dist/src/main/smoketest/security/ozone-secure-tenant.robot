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
Secure Tenant Create Tenant Success with Cluster Admin
    Run Keyword         Init Ranger MockServer
    Run Keyword   Kinit test user     testuser     testuser.keytab
    ${output} =         Execute          ozone tenant create tenantone
                        Should contain   ${output}         Created tenant 'tenantone'

Secure Tenant Assign User Success
    ${output} =         Execute          ozone tenant user assign bob --tenant=tenantone
                        Should contain   ${output}         Assigned 'bob' to 'tenantone'

Secure Tenant GetUserInfo Success
    ${output} =         Execute          ozone tenant user info bob
                        Should contain   ${output}         Tenant 'tenantone' with accessId 'tenantone$bob'

Secure Tenant SetSecret Success with Cluster Admin
    ${output} =         Execute          ozone tenant user setsecret 'tenantone$bob' --secret=somesecret1 --export
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

Secure Tenant SetSecret Failure For Invalid Secret Input 1
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret 'tenantone$bob' --secret='' --export
                        Should contain   ${output}         secretKey cannot be null or empty.

Secure Tenant SetSecret Failure For Invalid Secret Input 2
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret 'tenantone$bob' --secret=short --export
                        Should contain   ${output}         Secret key length should be at least 8 characters

Secure Tenant GetSecret Success
    ${output} =         Execute          ozone tenant user getsecret 'tenantone$bob' --export
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

Secure Tenant Delete Tenant Failure Tenant Not Empty
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant delete tenantone
                        Should contain   ${output}         Failed to delete tenant 'tenantone': Tenant 'tenantone' is not empty

Secure Tenant Revoke User AccessId Success
    ${output} =         Execute          ozone tenant user revoke 'tenantone$bob'
                        Should contain   ${output}         Revoked accessId 'tenantone$bob'.

Secure Tenant Delete Tenant Success
    ${output} =         Execute          ozone tenant delete tenantone
                        Should contain   ${output}         Deleted tenant 'tenantone'.

Secure Tenant Assign User Failure
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user assign bob --tenant=thistenantdoesnotexist
                        Should contain   ${output}         Tenant 'thistenantdoesnotexist' doesn't exist

Secure Tenant Create Tenant Failure with Regular (non-admin) user
    Run Keyword   Kinit test user     testuser2    testuser2.keytab
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant create tenanttwo
                        Should contain   ${output}         Failed to create tenant 'tenanttwo': User 'testuser2/scm@EXAMPLE.COM' is not an Ozone admin.

Secure Tenant SetSecret Failure with Regular (non-admin) user
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user set-secret 'tenantone$bob' --secret=somesecret2 --export
                        Should contain   ${output}         Permission denied. Requested accessId
