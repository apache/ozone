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
${ICEBERG_SVC_CATALOG_USER}             svc-iceberg-rest-catalog
${ICEBERG_ALL_ACCESS_ROLE_OBS}          iceberg-data-all-access-obs
${ICEBERG_ALL_ACCESS_ROLE_FSO}          iceberg-data-all-access-fso
${ICEBERG_READ_ONLY_ROLE_OBS}           iceberg-data-read-only-obs
${ICEBERG_READ_ONLY_ROLE_FSO}           iceberg-data-read-only-fso
${ICEBERG_BUCKET_OBS}                   iceberg-obs
${ICEBERG_BUCKET_FSO}                   iceberg-fso
${ICEBERG_LAYOUT_OBS}                   OBJECT_STORE
${ICEBERG_LAYOUT_FSO}                   FILE_SYSTEM_OPTIMIZED
${ICEBERG_BUCKET_TESTFILE}              file1.txt
${ROLE_ARN_OBS}                         arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE_OBS}
${ROLE_ARN_FSO}                         arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE_FSO}
${READ_ONLY_ROLE_ARN_OBS}               arn:aws:iam::123456789012:role/${ICEBERG_READ_ONLY_ROLE_OBS}
${READ_ONLY_ROLE_ARN_FSO}               arn:aws:iam::123456789012:role/${ICEBERG_READ_ONLY_ROLE_FSO}
${TEST_USER_NON_ADMIN}                  testuser2
@{ICEBERG_OBJECT_KEYS}                  file1.txt    file1again.txt    folder/pepper.txt    folder/salt.txt    userA/userA.txt    userB/userB.txt    userAfile.txt
@{ICEBERG_LISTABLE_OBJECT_KEYS_OBS}     file1.txt    file1again.txt    folder/pepper.txt    folder/salt.txt    userA/userA.txt    userB/userB.txt    userAfile.txt    zeroByteFile    zeroByteFolder/
@{ICEBERG_LISTABLE_OBJECT_KEYS_FSO}     file1.txt    file1again.txt    folder/    folder/pepper.txt    folder/salt.txt    userA/    userA/userA.txt    userAfile.txt    userB/    userB/userB.txt    zeroByteFile    zeroByteFolder
@{ICEBERG_PREFIX_USERA_OBS}             userA/userA.txt
@{ICEBERG_PREFIX_USERA_FSO}             userA/    userA/userA.txt
@{ICEBERG_PREFIX_USERA_STAR_OBS}        userA/userA.txt    userAfile.txt
@{ICEBERG_PREFIX_USERA_STAR_FSO}        userA/    userA/userA.txt

*** Keywords ***
Populate Iceberg Bucket
    [Arguments]                   ${bucket}
    FOR    ${key}    IN    @{ICEBERG_OBJECT_KEYS}
        ${parent} =               Evaluate                      __import__('os').path.dirname($key)
        Run Keyword If            '${parent}' != ''             Execute    mkdir -p ${TEMP_DIR}/${parent}
        ${local_path} =           Set Variable                  ${TEMP_DIR}/${key}
        Create File               ${local_path}                 iceberg test content
        Execute                   ozone sh key put /s3v/${bucket}/${key} ${local_path}
    END
    Create File                   ${TEMP_DIR}/zeroByteFile
    Execute                       ozone sh key put /s3v/${bucket}/zeroByteFile ${TEMP_DIR}/zeroByteFile
    # Upload an explicit folder marker object to match AWS semantics.
    Create File                   ${TEMP_DIR}/zero-byte-marker
    Execute                       ozone sh key put /s3v/${bucket}/zeroByteFolder/ ${TEMP_DIR}/zero-byte-marker

Run List Prefix And Delimiter Policy Matrix For Bucket And Api
    [Arguments]                   ${bucket}  ${role_arn}  ${api}
    # Capture baseline (non-STS) behavior using the permanent credentials.  When using STS, it will behave just like
    # non-STS S3 behavior in terms of listing results, with the addition of checking authorization if any s3:prefix
    # was specified in the inline session policy
    Configure AWS Profile         permanent  ${PERMANENT_ACCESS_KEY_ID}  ${PERMANENT_SECRET_KEY}
    ${baseline_no_prefix_no_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  profile=permanent
    ${baseline_userA_slash_prefix_no_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA/  profile=permanent
    ${baseline_userA_prefix_no_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA  profile=permanent
    ${baseline_no_prefix_slash_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  profile=permanent  delimiter=/
    ${baseline_userA_slash_prefix_slash_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA/  profile=permanent  delimiter=/
    ${baseline_userA_prefix_slash_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA  profile=permanent  delimiter=/
    IF                            '${bucket}' == '${ICEBERG_BUCKET_OBS}'
        Assert Listed Keys Should Equal  ${baseline_no_prefix_no_delimiter}  @{ICEBERG_LISTABLE_OBJECT_KEYS_OBS}
        Assert Listed Keys Should Equal  ${baseline_userA_slash_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_OBS}
        Assert Listed Keys Should Equal  ${baseline_userA_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_STAR_OBS}
    ELSE IF                       '${bucket}' == '${ICEBERG_BUCKET_FSO}'
        Assert Listed Keys Should Equal  ${baseline_no_prefix_no_delimiter}  @{ICEBERG_LISTABLE_OBJECT_KEYS_FSO}
        Assert Listed Keys Should Equal  ${baseline_userA_slash_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_FSO}
        Assert Listed Keys Should Equal  ${baseline_userA_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_STAR_FSO}
    END

    # a) IAM session policy with no s3:prefix condition.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  profile=sts  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}

    # b1) StringEquals without wildcard.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":"userA/"}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    # If a prefix was authorized in the session policy, but the user did not supply any prefix, verify access denied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    # If a prefix was authorized in the session policy, but the user supplied a different prefix, verify access denied
    # (userA does not match userA/)
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA

    # b2) StringEquals with wildcard only, wildcard prefix should be ignored and deny.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":"userA/*"}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA/  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # b3) StringEquals mixed values, wildcard entry is ignored but exact entry still works.  Ranger doesn't support
    # literal asterisk matching
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":["userA/","userA/*"]}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # c1) StringLike without wildcard.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringLike":{"s3:prefix":"userA/"}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # c2) StringLike with wildcard and slash (userA/* edge case).
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringLike":{"s3:prefix":"userA/*"}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # c3) StringLike with wildcard only (userA* edge case).
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringLike":{"s3:prefix":"userA*"}}}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/

    # d) No IAM session policy (it should work just like the baseline)
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}

*** Test Cases ***
Create User in Ranger
    ${user_json} =                Set Variable                  { "loginId": "${ICEBERG_SVC_CATALOG_USER}", "name": "${ICEBERG_SVC_CATALOG_USER}", "password": "Password123", "firstName": "Iceberg REST", "lastName": "Catalog", "emailAddress": "${ICEBERG_SVC_CATALOG_USER}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }
    Create Ranger User            ${user_json}

Create All Access Roles in Ranger
    FOR    ${role}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}
        ${role_json} =            Set Variable                  { "name": "${role}", "description": "Iceberg data all access" }
        Create Ranger Role        ${role_json}
    END

Create Read Only Roles in Ranger
    FOR    ${role}    IN    ${ICEBERG_READ_ONLY_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${role_json} =            Set Variable                  { "name": "${role}", "description": "Iceberg data read only" }
        Create Ranger Role        ${role_json}
    END

Create All Access Assume Role Policies
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on each all-access role.
    FOR    ${role}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${role} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${role}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Create Read Only Assume Role Policies
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on each read-only role.
    FOR    ${role}    IN    ${ICEBERG_READ_ONLY_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${role} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${role}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Create Iceberg Volume Access Policy
    # This policy gives all iceberg roles READ,LIST permission on volume s3v.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ permission on volume s3v.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg volume access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE_OBS}", "${ICEBERG_ALL_ACCESS_ROLE_FSO}", "${ICEBERG_READ_ONLY_ROLE_OBS}", "${ICEBERG_READ_ONLY_ROLE_FSO}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Bucket Access Policies
    # This loop gives '${ICEBERG_ALL_ACCESS_ROLE_OBS}' ALL permission on '${ICEBERG_BUCKET_OBS}', '${ICEBERG_SVC_CATALOG_USER}' user READ, CREATE permission on '${ICEBERG_BUCKET_OBS}', '${ICEBERG_READ_ONLY_ROLE_OBS}' READ permission on '${ICEBERG_BUCKET_OBS}'
    # It also gives '${ICEBERG_ALL_ACCESS_ROLE_FSO}' ALL permission on '${ICEBERG_BUCKET_FSO}', ${ICEBERG_SVC_CATALOG_USER}' user READ, CREATE permission on '${ICEBERG_BUCKET_FSO}', '${ICEBERG_READ_ONLY_ROLE_FSO}' READ permission on '${ICEBERG_BUCKET_FSO}'
    FOR    ${bucket}    ${all_access_role}    ${read_only_role}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg ${bucket} bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${bucket}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${all_access_role}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${read_only_role}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Create Iceberg Table Access Policies
    # This loop gives '${ICEBERG_ALL_ACCESS_ROLE_OBS}' ALL permission on '${ICEBERG_BUCKET_OBS}'/* keys, '${ICEBERG_READ_ONLY_ROLE_OBS}' READ permission on '${ICEBERG_BUCKET_OBS}'/* keys
    # It also gives '${ICEBERG_ALL_ACCESS_ROLE_FSO}' ALL permission on '${ICEBERG_BUCKET_FSO}'/* keys, '${ICEBERG_READ_ONLY_ROLE_FSO}' READ permission on '${ICEBERG_BUCKET_FSO}'/* keys
    FOR    ${bucket}    ${all_access_role}    ${read_only_role}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg ${bucket} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${bucket}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${all_access_role}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${read_only_role}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Get S3 Credentials for Service Catalog Principal, Create Iceberg Buckets, and Upload Files
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    # Waiting for Ranger policy cache refresh - ${ICEBERG_SVC_CATALOG_USER} needs to be able to read s3v volume
    Wait Until Keyword Succeeds    30s    5s                    Execute    ozone sh volume info s3v

    ${output} =                   Execute                       ozone s3 getsecret
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=awsAccessKey=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=awsSecret=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}
    Set Global Variable           ${PERMANENT_ACCESS_KEY_ID}    ${accessKeyId}
    Set Global Variable           ${PERMANENT_SECRET_KEY}       ${secretKey}
    Execute                       ozone sh bucket create --layout ${ICEBERG_LAYOUT_OBS} /s3v/${ICEBERG_BUCKET_OBS}
    Execute                       ozone sh bucket create --layout ${ICEBERG_LAYOUT_FSO} /s3v/${ICEBERG_BUCKET_FSO}
    Populate Iceberg Bucket       ${ICEBERG_BUCKET_OBS}
    Populate Iceberg Bucket       ${ICEBERG_BUCKET_FSO}

Assume Role for Limited-Scope Token
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role for Role-Scoped Token
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
        Put Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
    END

Assume Role with Invalid Action in Session Policy
    # s3:InvalidAction in the session policy is not valid
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:InvalidAction","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Mismatched Action and Resource in Session Policy
    # s3:GetObject is for object resources but the Resource "arn:aws:s3:::bucket" is a bucket resource.
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}"}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Elevated Access in Session Policy Should Fail
    # Assume read-only role but try to grant write access in session policy - this should not be allowed
    FOR    ${bucket}    ${read_only_role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${READ_ONLY_ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${READ_ONLY_ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${read_only_role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Malformed Session Policy JSON Should Fail
    ${session_policy} =           Set Variable                  {"ThisIsMalformed"}
    FOR    ${role_arn}    IN    ${ROLE_ARN_OBS}    ${ROLE_ARN_FSO}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  policy_json=${session_policy}  expected_error=MalformedPolicyDocument  expected_http_code=400  role_arn=${role_arn}
    END

Assume Role with Unsupported Condition Operator in Session Policy Should Fail
    # StringNotEqualsIgnoreCase is currently unsupported
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*","Condition":{"StringNotEqualsIgnoreCase":{"s3:prefix":"my_table/*"}}}]}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  policy_json=${session_policy}  expected_error=UnsupportedOperation  expected_http_code=501  role_arn=${role_arn}
    END

Assume Role with GetObject in Session Policy Using s3:prefix Should Fail
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*","Condition":{"StringLike":{"s3:prefix":"file1*"}}}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with GetObject in Session Policy Should Not Allow List Buckets
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Get Temporary Credentials               policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        List Object Keys Should Fail  ${bucket}  list-objects  AccessDenied
        List Object Keys Should Fail  ${bucket}  list-objects-v2  AccessDenied
    END

STS Token with Bogus AccessKeyId, Valid SecretKey and Valid SessionToken Should Fail
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        # Use a bogus accessKeyId instead of the valid ${STS_ACCESS_KEY_ID}
        Configure STS Profile     bogusAccessKeyId  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

STS Token with Valid AccessKeyId, Bogus SecretKey and Valid SessionToken Should Fail
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        # Use a bogus secretKey instead of the valid ${STS_SECRET_KEY}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  bogusSecretKey  ${STS_SESSION_TOKEN}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

STS Token with Valid AccessKeyId, Valid SecretKey and Bogus SessionToken Should Fail
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        # Use a bogus sessionToken instead of the valid ${STS_SESSION_TOKEN}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  bogusSessionToken
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role Without Ranger Permission Should Fail
    # A user who doesn't have assume role permission in Ranger should not be able to invoke the api
    Kinit test user               ${TEST_USER_NON_ADMIN}        ${TEST_USER_NON_ADMIN}.keytab

    ${output} =                   Execute                       ozone s3 getsecret
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=awsAccessKey=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=awsSecret=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}

    FOR    ${role_arn}    IN    ${ROLE_ARN_OBS}    ${ROLE_ARN_FSO}
        Assume Role Should Fail   perm_access_key_id=${accessKeyId}  perm_secret_key=${secretKey}  expected_error=AccessDenied  role_arn=${role_arn}
    END

Assume Role With Incorrect Permanent Credentials Should Fail
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    FOR    ${role_arn}    IN    ${ROLE_ARN_OBS}    ${ROLE_ARN_FSO}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=InvalidSecretKey  expected_error=InvalidClientTokenId  role_arn=${role_arn}
    END

STS Token with Presigned URL
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        ${presigned_url} =        Execute                       aws s3 presign s3://${bucket}/${ICEBERG_BUCKET_TESTFILE} --endpoint-url ${S3G_ENDPOINT_URL} --profile sts
        Should Contain            ${presigned_url}              X-Amz-Algorithm=AWS4-HMAC-SHA256
        Should Contain            ${presigned_url}              X-Amz-Security-Token=
        ${output} =               Execute                       curl -v '${presigned_url}'
        Should Contain            ${output}                     HTTP/1.1 200 OK
    END

Verify Token Revocation via CLI
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
        ${output} =               Execute                       ozone s3 revokeststoken -t ${STS_SESSION_TOKEN} -y ${OM_HA_PARAM}
        Should Contain            ${output}                     STS token revoked for sessionToken
        # Trying to use the token for even get-object should now fail.
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Non-Admin Cannot Revoke STS Token
    FOR    ${role_arn}    IN    ${ROLE_ARN_OBS}    ${ROLE_ARN_FSO}
        # Create a token first.
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        ${token_to_revoke} =      Set Variable                  ${STS_SESSION_TOKEN}

        # Kinit as non-admin user.
        Kinit test user           ${TEST_USER_NON_ADMIN}        ${TEST_USER_NON_ADMIN}.keytab

        # Try to revoke - should give USER_MISMATCH error.
        ${output} =               Execute And Ignore Error      ozone s3 revokeststoken -t ${token_to_revoke} -y ${OM_HA_PARAM}
        Should Contain            ${output}                     USER_MISMATCH
    END

List Objects V1 and V2 IAM Session Policy Matrix for OBS and FSO
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ROLE_ARN_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ROLE_ARN_FSO}
        FOR    ${api}    IN    list-objects    list-objects-v2
            Run List Prefix And Delimiter Policy Matrix For Bucket And Api    ${bucket}  ${role_arn}  ${api}
        END
    END

Revoking Permanent User Must Revoke Existing Session Token
    # Create session tokens for both buckets, verify they work, then revoke permanent user secret and verify both fail.
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ROLE_ARN_OBS}
    Set Test Variable             ${OBS_STS_ACCESS_KEY_ID}      ${STS_ACCESS_KEY_ID}
    Set Test Variable             ${OBS_STS_SECRET_KEY}         ${STS_SECRET_KEY}
    Set Test Variable             ${OBS_STS_SESSION_TOKEN}      ${STS_SESSION_TOKEN}
    Configure STS Profile         ${OBS_STS_ACCESS_KEY_ID}  ${OBS_STS_SECRET_KEY}  ${OBS_STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}

    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ROLE_ARN_FSO}
    Set Test Variable             ${FSO_STS_ACCESS_KEY_ID}      ${STS_ACCESS_KEY_ID}
    Set Test Variable             ${FSO_STS_SECRET_KEY}         ${STS_SECRET_KEY}
    Set Test Variable             ${FSO_STS_SESSION_TOKEN}      ${STS_SESSION_TOKEN}
    Configure STS Profile         ${FSO_STS_ACCESS_KEY_ID}  ${FSO_STS_SECRET_KEY}  ${FSO_STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_FSO}  ${ICEBERG_BUCKET_TESTFILE}

    # Log in again as user who owns the ${PERMANENT_ACCESS_KEY_ID} so we can issue revokesecret command.
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab
    Execute                       ozone s3 revokesecret -y -u ${PERMANENT_ACCESS_KEY_ID} ${OM_HA_PARAM}

    # Session tokens must no longer work.
    Configure STS Profile         ${OBS_STS_ACCESS_KEY_ID}  ${OBS_STS_SECRET_KEY}  ${OBS_STS_SESSION_TOKEN}
    Get Object Should Fail        ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Configure STS Profile         ${FSO_STS_ACCESS_KEY_ID}  ${FSO_STS_SECRET_KEY}  ${FSO_STS_SESSION_TOKEN}
    Get Object Should Fail        ${ICEBERG_BUCKET_FSO}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
