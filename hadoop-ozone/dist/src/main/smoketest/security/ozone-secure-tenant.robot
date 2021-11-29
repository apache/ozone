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
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***
${RANGER_ENDPOINT_URL}  https://ranger:6182
${S3G_ENDPOINT_URL}     http://s3g:9878

*** Keywords ***
Init Ranger MockServer
    ${output} =         Execute          curl -k ${RANGER_ENDPOINT_URL}
                        Should contain   ${output}         {}

*** Test Cases ***
Secure Tenant Create Tenant Success with Cluster Admin
    Run Keyword         Init Ranger MockServer
    Run Keyword         Kinit test user     testuser     testuser.keytab
    ${output} =         Execute          ozone tenant create tenantone
                        Should contain   ${output}         Created tenant 'tenantone'

Secure Tenant Assign User Success with Cluster Admin
    ${output} =         Execute          ozone tenant user assign testuser --tenant=tenantone
                        Should contain   ${output}         Assigned 'testuser' to 'tenantone'
    ${accessId} =       Get Regexp Matches   ${output}     (?<=export AWS_ACCESS_KEY_ID=).*
    ${secretKey} =      Get Regexp Matches   ${output}     (?<=export AWS_SECRET_ACCESS_KEY=).*
    ${accessId} =       Set Variable         ${accessId[0]}
    ${secretKey} =      Set Variable         ${secretKey[0]}
                        Set Global Variable  ${ACCESS_ID}   ${accessId}
                        Set Global Variable  ${SECRET_KEY}  ${secretKey}

Secure Tenant Assign User Failure to Non-existent Tenant
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user assign testuser --tenant=thistenantdoesnotexist
                        Should contain   ${output}         Tenant 'thistenantdoesnotexist' doesn't exist

Secure Tenant GetUserInfo Success
    ${output} =         Execute          ozone tenant user info testuser
                        Should contain   ${output}         Tenant 'tenantone' with accessId 'tenantone$testuser'

Secure Tenant Create Bucket 1 Success via S3 API
    Run Keyword         Setup s3 tests
                        Execute          aws configure set aws_access_key_id ${ACCESS_ID}
                        Execute          aws configure set aws_secret_access_key ${SECRET_KEY}
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test1
                        Should contain   ${output}         bucket-test1
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-buckets
                        Should contain   ${output}         bucket-test1

Secure Tenant SetSecret Success with Cluster Admin
    ${output} =         Execute          ozone tenant user setsecret 'tenantone$testuser' --secret=somesecret1 --export
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

Secure Tenant SetSecret Failure For Invalid Secret 1
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret 'tenantone$testuser' --secret='' --export
                        Should contain   ${output}         secretKey cannot be null or empty.

Secure Tenant SetSecret Failure For Invalid Secret 2
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret 'tenantone$testuser' --secret=short --export
                        Should contain   ${output}         Secret key length should be at least 8 characters

Secure Tenant GetSecret Success
    ${output} =         Execute          ozone tenant user getsecret 'tenantone$testuser' --export
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

Secure Tenant Delete Bucket 1 Failure With Old SecretKey via S3 API
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test1
                        Should Be True	${rc} > 0

Secure Tenant Delete Bucket 1 Success With Newly Set SecretKey via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret1'
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test1
                        Should contain   ${output}         bucket-test1

Secure Tenant Delete Tenant Failure Tenant Not Empty
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant delete tenantone
                        Should contain   ${output}         Failed to delete tenant 'tenantone': Tenant 'tenantone' is not empty

Secure Tenant Create Tenant Failure with Regular User
    Run Keyword         Kinit test user     testuser2    testuser2.keytab
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant create tenanttwo
                        Should contain   ${output}         Failed to create tenant 'tenanttwo': User 'testuser2/scm@EXAMPLE.COM' is not an Ozone admin.

Secure Tenant SetSecret Failure with Regular User
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user set-secret 'tenantone$testuser' --secret=somesecret2 --export
                        Should contain   ${output}         Permission denied. Requested accessId

Secure Tenant Create Bucket 2 Success with somesecret1 via S3 API
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test2
                        Should contain   ${output}         bucket-test2

Secure Tenant Delete Bucket 2 Failure with somesecret2 via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret2'
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test2
                        Should Be True	${rc} > 0

Secure Tenant Delete Bucket 2 Success with somesecret1 via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret1'
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test2
                        Should contain   ${output}         bucket-test2

Secure Tenant Revoke User AccessId Success with Cluster Admin
    Run Keyword         Kinit test user     testuser     testuser.keytab
    ${output} =         Execute          ozone tenant user revoke 'tenantone$testuser'
                        Should contain   ${output}         Revoked accessId 'tenantone$testuser'.

Secure Tenant Create Bucket 3 Failure with Revoked AccessId via S3 API
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test3
                        Should Be True	${rc} > 0

Secure Tenant Delete Tenant Success with Cluster Admin
    ${output} =         Execute          ozone tenant delete tenantone
                        Should contain   ${output}         Deleted tenant 'tenantone'.

Secure Tenant List Tenant Expect Empty Result
    ${output} =         Execute          ozone tenant list
                        Should not contain   ${output}     tenantone
