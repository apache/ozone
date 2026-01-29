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
Documentation       Smoke test for S3 STS AssumeRole + Temp Creds
Library             OperatingSystem
Library             String
Library             BuiltIn
Library             DateTime
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        10 minutes

*** Variables ***
${RANGER_ENDPOINT_URL}       ${EMPTY}
${STS_ENDPOINT_URL}          http://s3g:9880/sts
${S3G_ENDPOINT_URL}          http://s3g:9878

${ICEBERG_SVC_CATALOG_USER}  svc-iceberg-rest-catalog
${ICEBERG_ALL_ACCESS_ROLE}   iceberg-data-all-access
${ICEBERG_BUCKET}            iceberg
${ICEBERG_BUCKET_TESTFILE}   testfile55
${ROLE_ARN}                  arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE}
${ROLE_SESSION_NAME}         sts-session-name

*** Keywords ***
Create Ranger Policy
    [Arguments]       ${policy_json}
    ${result} =       Execute    curl --fail --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '${policy_json}' '${RANGER_ENDPOINT_URL}/service/public/v2/api/policy'
    Should Contain    ${result}    HTTP/1.1 200

Assume Role And Get Temporary Credentials
    [Arguments]           ${policy_json}=${EMPTY}
    Run Keyword           Install aws cli
    Execute               aws configure set aws_access_key_id ${PERMANENT_ACCESS_ID} --profile permanent
    Execute               aws configure set aws_secret_access_key ${PERMANENT_SECRET_KEY} --profile permanent
    Execute               aws configure set region us-east-1 --profile permanent

    ${now} =              Get Current Date    time_zone=UTC

    ${cmd} =              Set Variable       aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${ROLE_ARN} --role-session-name ${ROLE_SESSION_NAME} --duration-seconds 900 --output json --profile permanent
    ${cmd} =              Set Variable If    '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

    ${json} =             Execute             ${cmd}
                          Should contain      ${json}  Credentials
    ${stsAccessId} =      Execute             echo '${json}' | jq -r '.Credentials.AccessKeyId'
    ${stsSecretKey} =     Execute             echo '${json}' | jq -r '.Credentials.SecretAccessKey'
    ${stsSessionToken} =  Execute             echo '${json}' | jq -r '.Credentials.SessionToken'
                          Should Start With   ${stsAccessId}   ASIA
                          Set Global Variable  ${STS_ACCESS_ID}   ${stsAccessId}
                          Set Global Variable  ${STS_SECRET_KEY}  ${stsSecretKey}
                          Set Global Variable  ${STS_SESSION_TOKEN}  ${stsSessionToken}

    # Verify Expiration is at least 900 seconds in the future, but allow 2 second grace for clock skew
    ${expiration} =       Execute             echo '${json}' | jq -r '.Credentials.Expiration'
    ${time_diff} =        Subtract Date From Date    ${expiration}    ${now}
    Should Be True        ${time_diff} >= 898      Expected expiration to be at least 898s in the future, but was ${time_diff}s

Configure STS Profile
    Execute             aws configure set aws_access_key_id ${STS_ACCESS_ID} --profile sts
    Execute             aws configure set aws_secret_access_key ${STS_SECRET_KEY} --profile sts
    Execute             aws configure set aws_session_token ${STS_SESSION_TOKEN} --profile sts

*** Test Cases ***
Create Users in Ranger
    Pass Execution If    '${RANGER_ENDPOINT_URL}' == ''    No Ranger
    # Note: the /service/xusers/secure/users endpoint must be used below so that the userPermList can be set.  Without
    # the userPermList being set, the user cannot be added to a Ranger policy.
    Execute       curl --fail --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '{ "loginId": "${ICEBERG_SVC_CATALOG_USER}", "name": "${ICEBERG_SVC_CATALOG_USER}", "password": "Password123", "firstName": "Iceberg REST", "lastName": "Catalog", "emailAddress": "${ICEBERG_SVC_CATALOG_USER}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }' '${RANGER_ENDPOINT_URL}/service/xusers/secure/users'

Create Role in Ranger
    Pass Execution If    '${RANGER_ENDPOINT_URL}' == ''    No Ranger
    Execute    curl --fail --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '{"name": "${ICEBERG_ALL_ACCESS_ROLE}", "description": "Iceberg data all access"}' '${RANGER_ENDPOINT_URL}/service/roles/roles'

Create Assume Role Policy
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on role '${ICEBERG_ALL_ACCESS_ROLE}'
    ${policy_json} =    Set Variable    { "isEnabled": true, "service": "dev_ozone", "name": "iceberg role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${ICEBERG_ALL_ACCESS_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy    ${policy_json}

Create Iceberg Volume Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role READ,LIST permission on volume s3v.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ permission on volume s3v.
    ${policy_json} =    Set Variable    { "isEnabled": true, "service": "dev_ozone", "name": "iceberg volume access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy    ${policy_json}

Create Iceberg Bucket Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role ALL permission on bucket s3v/${ICEBERG_BUCKET}.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ permission on bucket s3v/${ICEBERG_BUCKET}.
    ${policy_json} =    Set Variable    { "isEnabled": true, "service": "dev_ozone", "name": "iceberg bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy    ${policy_json}

Create Iceberg Table Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role ALL permission on keys s3v/${ICEBERG_BUCKET}/*.
    ${policy_json} =    Set Variable    { "isEnabled": true, "service": "dev_ozone", "name": "iceberg table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy    ${policy_json}

Get S3 Credentials for Service Catalog Principal, Create Iceberg Bucket, and Upload File to Bucket
    Kinit test user     ${ICEBERG_SVC_CATALOG_USER}     ${ICEBERG_SVC_CATALOG_USER}.keytab
    # Waiting for Ranger policy cache refresh - ${ICEBERG_SVC_CATALOG_USER} needs to be able to read s3v volume
    Wait Until Keyword Succeeds    30s    5s    Execute    ozone sh volume info s3v
    ${output} =         Execute  ozone s3 getsecret
    ${accessId} =       Get Regexp Matches   ${output}     (?<=awsAccessKey=).*
    ${secretKey} =      Get Regexp Matches   ${output}     (?<=awsSecret=).*
    ${accessId} =       Set Variable         ${accessId[0]}
    ${secretKey} =      Set Variable         ${secretKey[0]}
                        Set Global Variable  ${PERMANENT_ACCESS_ID}   ${accessId}
                        Set Global Variable  ${PERMANENT_SECRET_KEY}  ${secretKey}
    Execute             ozone sh bucket create /s3v/${ICEBERG_BUCKET}
    Create File         ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}
    Execute             ozone sh key put /s3v/${ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}

AssumeRole for Limited-Scope Token
    ${session_policy} =         Set Variable    {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::iceberg/*"}]}
    Assume Role And Get Temporary Credentials    policy_json=${session_policy}

Verify Limited-Scope Token Access
    Configure STS Profile
    ${bucketSuffix} =   Generate Random String   8   [LOWER]
    ${bucket} =         Set Variable             sts-bucket-${bucketSuffix}
    ${output} =         Execute And Ignore Error     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket ${bucket} --profile sts
                        Should contain   ${output}         AccessDenied
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object --bucket ${ICEBERG_BUCKET} --key ${ICEBERG_BUCKET_TESTFILE} ./${ICEBERG_BUCKET_TESTFILE} --profile sts
                        Should contain   ${output}         "AcceptRanges": "bytes"
    ${output} =         Execute And Ignore Error     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object --bucket ${ICEBERG_BUCKET} --key ${ICEBERG_BUCKET_TESTFILE} --body ./${ICEBERG_BUCKET_TESTFILE} --profile sts
                        Should contain   ${output}         AccessDenied

AssumeRole for Role-Scoped Token
    Assume Role And Get Temporary Credentials

Verify Role-Scoped Token Access
    Configure STS Profile
    ${bucketSuffix} =   Generate Random String   8   [LOWER]
    ${bucket} =         Set Variable             sts-bucket-${bucketSuffix}
    ${output} =         Execute And Ignore Error     aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket ${bucket} --profile sts
                        Should contain   ${output}         AccessDenied
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object --bucket ${ICEBERG_BUCKET} --key ${ICEBERG_BUCKET_TESTFILE} ./${ICEBERG_BUCKET_TESTFILE} --profile sts
                        Should contain   ${output}         "AcceptRanges": "bytes"
    ${output} =         Execute          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object --bucket ${ICEBERG_BUCKET} --key ${ICEBERG_BUCKET_TESTFILE} --body ./${ICEBERG_BUCKET_TESTFILE} --profile sts
                        Should contain   ${output}         "ETag"

Verify Token Revocation
    ${output} =         Execute  ozone s3 revokeststoken -t ${STS_SESSION_TOKEN} -y ${OM_HA_PARAM}
                        Should contain   ${output}        STS token revoked for sessionToken
    # Trying to use the token for even get-object should now fail
    ${output} =         Execute And Ignore Error          aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object --bucket ${ICEBERG_BUCKET} --key ${ICEBERG_BUCKET_TESTFILE} ./${ICEBERG_BUCKET_TESTFILE} --profile sts
                        Should contain   ${output}        AccessDenied
