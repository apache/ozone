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
Resource            ./ozone-secure-sts.resource
Test Timeout        10 minutes

*** Variables ***
${ICEBERG_SVC_CATALOG_USER}       svc-iceberg-rest-catalog
${ICEBERG_ALL_ACCESS_ROLE}        iceberg-data-all-access
${ICEBERG_READ_ONLY_ROLE}         iceberg-data-read-only
${ICEBERG_BUCKET}                 iceberg
${ICEBERG_BUCKET_TESTFILE}        testfile55
${ROLE_ARN}                       arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE}
${READ_ONLY_ROLE_ARN}             arn:aws:iam::123456789012:role/${ICEBERG_READ_ONLY_ROLE}
${TEST_USER_NON_ADMIN}            testuser2

*** Test Cases ***
Create User in Ranger
    ${user_json} =                Set Variable                  { "loginId": "${ICEBERG_SVC_CATALOG_USER}", "name": "${ICEBERG_SVC_CATALOG_USER}", "password": "Password123", "firstName": "Iceberg REST", "lastName": "Catalog", "emailAddress": "${ICEBERG_SVC_CATALOG_USER}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }
    Create Ranger User            ${user_json}

Create All Access Role in Ranger
    ${role_json} =                Set Variable                  { "name": "${ICEBERG_ALL_ACCESS_ROLE}", "description": "Iceberg data all access" }
    Create Ranger Role            ${role_json}

Create Read Only Role in Ranger
    ${role_json} =                Set Variable                  { "name": "${ICEBERG_READ_ONLY_ROLE}", "description": "Iceberg data read only" }
    Create Ranger Role            ${role_json}

Create All Access Assume Role Policy
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on role '${ICEBERG_ALL_ACCESS_ROLE}'
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${ICEBERG_ALL_ACCESS_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Read Only Assume Role Policy
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on role '${ICEBERG_READ_ONLY_ROLE}'
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg read only assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${ICEBERG_READ_ONLY_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Volume Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role READ,LIST permission on volume s3v.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ permission on volume s3v.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg volume access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Bucket Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role ALL permission on bucket s3v/${ICEBERG_BUCKET}.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ, CREATE permissions on bucket s3v/${ICEBERG_BUCKET}.
    # It also gives '${ICEBERG_READ_ONLY_ROLE}' role READ permission on bucket s3v/${ICEBERG_BUCKET}.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${ICEBERG_READ_ONLY_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Table Access policy
    # This policy gives '${ICEBERG_ALL_ACCESS_ROLE}' role ALL permission on keys s3v/${ICEBERG_BUCKET}/*.
    # It also gives '${ICEBERG_READ_ONLY_ROLE}' READ permission on keys s3v/${ICEBERG_BUCKET}/*.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${ICEBERG_READ_ONLY_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Get S3 Credentials for Service Catalog Principal, Create Iceberg Bucket, and Upload File to Bucket
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    # Waiting for Ranger policy cache refresh - ${ICEBERG_SVC_CATALOG_USER} needs to be able to read s3v volume
    Wait Until Keyword Succeeds    30s    5s    Execute    ozone sh volume info s3v

    ${output} =                   Execute                       ozone s3 getsecret
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=awsAccessKey=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=awsSecret=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}
    Set Global Variable           ${PERMANENT_ACCESS_KEY_ID}    ${accessKeyId}
    Set Global Variable           ${PERMANENT_SECRET_KEY}       ${secretKey}
    Execute                       ozone sh bucket create /s3v/${ICEBERG_BUCKET}
    Create File                   ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}
    Execute                       ozone sh key put /s3v/${ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}

AssumeRole for Limited-Scope Token
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET}/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${bucketSuffix} =             Generate Random String        8   [LOWER]
    ${bucket} =                   Set Variable                  sts-bucket-${bucketSuffix}
    Create Bucket Should Fail     ${bucket}
    Get Object Should Succeed     ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Put Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

AssumeRole for Role-Scoped Token
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${bucketSuffix} =             Generate Random String        8   [LOWER]
    ${bucket} =                   Set Variable                  sts-bucket-${bucketSuffix}
    Create Bucket Should Fail     ${bucket}
    Get Object Should Succeed     ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Put Object Should Succeed     ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}

AssumeRole with Invalid Action in Session Policy
    # s3:InvalidAction in the session policy is not valid
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:InvalidAction","Resource":"arn:aws:s3:::${ICEBERG_BUCKET}/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

AssumeRole with Mismatched Action and Resource in Session Policy
    # s3:GetObject is for object resources but the Resource "arn:aws:s3:::${ICEBERG_BUCKET}" is a bucket resource
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET}"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

AssumeRole with Elevated Access in Session Policy Should Fail
    # Assume read-only role but try to grant write access in session policy - this should not be allowed
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET}/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${READ_ONLY_ROLE_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

Assume Role Without Ranger Permission Should Fail
    # A user who doesn't have assume role permission in Ranger should not be able to invoke the api
    Kinit test user               ${TEST_USER_NON_ADMIN}        ${TEST_USER_NON_ADMIN}.keytab

    ${output} =                   Execute                       ozone s3 getsecret
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=awsAccessKey=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=awsSecret=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}

    Assume Role Should Fail       perm_access_key_id=${accessKeyId}  perm_secret_key=${secretKey}  expected_error=AccessDenied

Assume Role With Incorrect Permanent Credentials Should Fail
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    Assume Role Should Fail       perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=InvalidSecretKey  expected_error=InvalidClientTokenId

STS Token with Presigned URL
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}

    ${presigned_url} =            Execute                       aws s3 presign s3://${ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} --endpoint-url ${S3G_ENDPOINT_URL} --profile sts
    Should Contain                ${presigned_url}              X-Amz-Algorithm=AWS4-HMAC-SHA256
    Should Contain                ${presigned_url}              X-Amz-Security-Token=
    ${output} =                   Execute                       curl -v '${presigned_url}'
    Should Contain                ${output}                     HTTP/1.1 200 OK

Verify Token Revocation via CLI
    ${output} =                   Execute                       ozone s3 revokeststoken -t ${STS_SESSION_TOKEN} -y ${OM_HA_PARAM}
    Should contain                ${output}                     STS token revoked for sessionToken
    # Trying to use the token for even get-object should now fail
    Get Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

Non-Admin Cannot Revoke STS Token
    # Create a token first
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    ${token_to_revoke} =          Set Variable                  ${STS_SESSION_TOKEN}

    # Kinit as non-admin user
    Kinit test user               ${TEST_USER_NON_ADMIN}        ${TEST_USER_NON_ADMIN}.keytab

    # Try to revoke - should give USER_MISMATCH error:
    # USER_MISMATCH Requested accessId 'svc-iceberg-rest-catalog/s3g@EXAMPLE.COM' doesn't match current user
    # 'testuser2/s3g@EXAMPLE.COM', nor does current user has administrator privilege.
    ${output} =                   Execute And Ignore Error      ozone s3 revokeststoken -t ${token_to_revoke} -y ${OM_HA_PARAM}
    Should Contain                ${output}                     USER_MISMATCH

Revoking Permanent User Must Revoke Existing Session Token
    # Create another session token, verify it works, then revoke the S3 permanent user and verify the token no longer works
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}

    # Log in again as user who owns the ${PERMANENT_ACCESS_KEY_ID} so we can issue revokesecret command
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab
    Execute                       ozone s3 revokesecret -y -u ${PERMANENT_ACCESS_KEY_ID} ${OM_HA_PARAM}

    # Session token must no longer work
    Get Object Should Fail        ${ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
