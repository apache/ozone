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
Documentation       Smoke test for ozone secure tenant commands
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Resource            ../admincli/lib.resource
Test Timeout        5 minutes

*** Variables ***
${RANGER_ENDPOINT_URL}  ${EMPTY}
${S3G_ENDPOINT_URL}     http://s3g:9878
${TENANT}               tenantone
${USER}                 testuser

*** Test Cases ***
Create User in Ranger
    Pass Execution If    '${RANGER_ENDPOINT_URL}' == ''    No Ranger
    Execute    curl --fail --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '{"loginId": "${TENANT}", "password": "testpassword", "firstName": "Test", "lastName": "User", "emailAddress": "testuser@example.com"}' '${RANGER_ENDPOINT_URL}/service/users'

Create Tenant Success with Cluster Admin
    Kinit test user     ${USER}     ${USER}.keytab
    ${output} =         Execute          ozone tenant --verbose create ${TENANT}
                        Should contain   ${output}         "tenantId" : "${TENANT}"

Assign User Success with Cluster Admin
    ${output} =         Execute          ozone tenant --verbose user assign ${USER} --tenant=${TENANT}
                        Should contain   ${output}         Assigned '${USER}' to '${TENANT}'
    ${accessId} =       Get Regexp Matches   ${output}     (?<=export AWS_ACCESS_KEY_ID=).*
    ${secretKey} =      Get Regexp Matches   ${output}     (?<=export AWS_SECRET_ACCESS_KEY=).*
    ${accessId} =       Set Variable         ${accessId[0]}
    ${secretKey} =      Set Variable         ${secretKey[0]}
                        Set Global Variable  ${ACCESS_ID}   ${accessId}
                        Set Global Variable  ${SECRET_KEY}  ${secretKey}

Assign User Failure to Non-existent Tenant
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user assign ${USER} --tenant=thistenantdoesnotexist
                        Should contain   ${output}         Tenant 'thistenantdoesnotexist' doesn't exist

GetUserInfo Success
    ${output} =         Execute          ozone tenant user info ${USER}
                        Should contain   ${output}         Tenant '${TENANT}' with accessId '${TENANT}$${USER}'

GetUserInfo as JSON Success
    ${output} =         Execute          ozone tenant user info --json ${USER} | jq '.tenants | .[].accessId'
                        Should contain   ${output}         "${TENANT}$${USER}"

Create Bucket 1 Success via S3 API
                        Execute          aws configure set aws_access_key_id ${ACCESS_ID}
                        Execute          aws configure set aws_secret_access_key ${SECRET_KEY}
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test1
                        Should contain   ${output}         bucket-test1
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-buckets
                        Should contain   ${output}         bucket-test1

Verify Bucket 1 Owner
    ${result} =         Execute          ozone sh bucket info /${TENANT}/bucket-test1 | jq -r '.owner'
                        Should Be Equal  ${result}       ${USER}

Put a key in the tenant bucket
                        Execute                 echo "Randomtext" > /tmp/testfile
                        Execute and checkrc     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object --bucket bucket-test1 --key mykey --body /tmp/testfile  0

Verify Object Owner
    ${result} =         Execute          ozone sh key info /${TENANT}/bucket-test1/mykey | jq -r '.owner'
                        Should Be Equal  ${result}       ${USER}

Get and delete a key in the tenant bucket
                        Execute and checkrc     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} head-object --bucket bucket-test1 --key mykey  0
                        Execute and checkrc     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-object --bucket bucket-test1 --key mykey  0

SetSecret Success with Cluster Admin
    ${output} =         Execute          ozone tenant user setsecret '${TENANT}$${USER}' --secret=somesecret1
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

SetSecret Failure For Invalid Secret 1
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret '${TENANT}$${USER}' --secret=''
                        Should contain   ${output}         secretKey cannot be null or empty.

SetSecret Failure For Invalid Secret 2
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user setsecret '${TENANT}$${USER}' --secret=short
                        Should contain   ${output}         Secret key length should be at least 8 characters

GetSecret Success
    ${output} =         Execute          ozone tenant user getsecret '${TENANT}$${USER}'
                        Should contain   ${output}         export AWS_SECRET_ACCESS_KEY='somesecret1'

Delete Bucket 1 Failure With Old SecretKey via S3 API
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test1
                        Should Be True	${rc} > 0

Delete Bucket 1 Success With Newly Set SecretKey via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret1'
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test1

Delete Tenant Failure Tenant Not Empty
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant delete ${TENANT}
                        Should contain   ${output}         TENANT_NOT_EMPTY Tenant '${TENANT}' is not empty. All accessIds associated to this tenant must be revoked before the tenant can be deleted. See `ozone tenant user revoke`

Trigger and wait for background Sync to recover Policies and Roles in Authorizer
   ${om_param}=        Get OM Service Param
   ${rc}  ${output} =  Run And Return Rc And Output  ozone admin om updateranger ${om_param}
                       Should contain   ${output}         Operation completed successfully

Create Tenant Failure with Regular User
    Run Keyword         Kinit test user     testuser2    testuser2.keytab
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant create tenanttwo
                        Should contain   ${output}         PERMISSION_DENIED User 'testuser2' is not an Ozone admin

SetSecret Failure with Regular User
    ${rc}  ${output} =  Run And Return Rc And Output  ozone tenant user set-secret '${TENANT}$${USER}' --secret=somesecret2
                        Should contain   ${output}         USER_MISMATCH Requested accessId '${TENANT}$${USER}' doesn't belong to current user 'testuser2', nor does current user have Ozone or tenant administrator privilege

Create Bucket 2 Success with somesecret1 via S3 API
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test2
                        Should contain   ${output}         bucket-test2

Delete Bucket 2 Failure with somesecret2 via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret2'
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test2
                        Should Be True	${rc} > 0

Delete Bucket 2 Success with somesecret1 via S3 API
                        Execute          aws configure set aws_secret_access_key 'somesecret1'
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket bucket-test2

Revoke User AccessId Success with Cluster Admin
    Run Keyword         Kinit test user     ${USER}     ${USER}.keytab
    ${output} =         Execute          ozone tenant --verbose user revoke '${TENANT}$${USER}'
                        Should contain   ${output}         Revoked accessId '${TENANT}$${USER}'.

Create Bucket 3 Failure with Revoked AccessId via S3 API
    ${rc}  ${output} =  Run And Return Rc And Output  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket bucket-test3
                        Should Be True	${rc} > 0

Delete Tenant Success with Cluster Admin
    ${output} =         Execute          ozone tenant delete ${TENANT}
                        Should contain   ${output}         Deleted tenant '${TENANT}'.

Delete Volume Success with Cluster Admin
    ${output} =         Execute          ozone sh volume delete ${TENANT}
                        Should contain   ${output}         Volume ${TENANT} is deleted

List Tenant Expect Empty Result
    ${output} =         Execute          ozone tenant list
                        Should not contain   ${output}     ${TENANT}
