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
${ICEBERG_MULTI_BUCKET_ROLE}            iceberg-data-all-access-multi
# Role used for sts-bucket-* resources. The create/delete bucket smoke tests need it so they can create a 
# temporary bucket, exercise the session policy under test, and then clean up that bucket in the same test.
${STS_TEMP_BUCKET_ROLE}                 sts-temp-bucket-access
${ICEBERG_READ_ONLY_ROLE_OBS}           iceberg-data-read-only-obs
${ICEBERG_READ_ONLY_ROLE_FSO}           iceberg-data-read-only-fso
${ICEBERG_BUCKET_OBS}                   iceberg-obs
${ICEBERG_BUCKET_FSO}                   iceberg-fso
${ICEBERG_LAYOUT_OBS}                   OBJECT_STORE
${ICEBERG_LAYOUT_FSO}                   FILE_SYSTEM_OPTIMIZED
${ICEBERG_BUCKET_TESTFILE}              file1.txt
${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}      arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE_OBS}
${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}      arn:aws:iam::123456789012:role/${ICEBERG_ALL_ACCESS_ROLE_FSO}
${ICEBERG_MULTI_BUCKET_ROLE_ARN}        arn:aws:iam::123456789012:role/${ICEBERG_MULTI_BUCKET_ROLE}
${STS_TEMP_BUCKET_ROLE_ARN}             arn:aws:iam::123456789012:role/${STS_TEMP_BUCKET_ROLE}
${READ_ONLY_ROLE_OBS_ARN}               arn:aws:iam::123456789012:role/${ICEBERG_READ_ONLY_ROLE_OBS}
${READ_ONLY_ROLE_FSO_ARN}               arn:aws:iam::123456789012:role/${ICEBERG_READ_ONLY_ROLE_FSO}
${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE}       partial-list-all-buckets-vol-read
${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE}       partial-list-all-buckets-vol-list
${PARTIAL_BUCKET_READ_ROLE}                     partial-bucket-read
${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}       partial-bucket-read-upload-prefix
${PARTIAL_BUCKET_LIST_ROLE}                     partial-bucket-list
${PARTIAL_BUCKET_READ_ACL_ROLE}                 partial-bucket-read-acl
${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}           partial-put-object-key-create
${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}            partial-put-object-key-write
${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE_ARN}   arn:aws:iam::123456789012:role/${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE}
${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE_ARN}   arn:aws:iam::123456789012:role/${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE}
${PARTIAL_BUCKET_READ_ROLE_ARN}                 arn:aws:iam::123456789012:role/${PARTIAL_BUCKET_READ_ROLE}
${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE_ARN}   arn:aws:iam::123456789012:role/${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}
${PARTIAL_BUCKET_LIST_ROLE_ARN}                 arn:aws:iam::123456789012:role/${PARTIAL_BUCKET_LIST_ROLE}
${PARTIAL_BUCKET_READ_ACL_ROLE_ARN}             arn:aws:iam::123456789012:role/${PARTIAL_BUCKET_READ_ACL_ROLE}
${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE_ARN}       arn:aws:iam::123456789012:role/${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}
${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE_ARN}        arn:aws:iam::123456789012:role/${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}
${TEST_USER_NON_ADMIN}                  testuser2
@{ICEBERG_OBJECT_KEYS}                  file1.txt    file1again.txt    folder/pepper.txt    folder/salt.txt    userA/userA.txt    userB/userB.txt    userAfile.txt
@{ICEBERG_LISTABLE_OBJECT_KEYS_OBS}     file1.txt    file1again.txt    folder/pepper.txt    folder/salt.txt    userA/userA.txt    userB/userB.txt    userAfile.txt    zeroByteFile    zeroByteFolder/
@{ICEBERG_LISTABLE_OBJECT_KEYS_FSO}     file1.txt    file1again.txt    folder/    folder/pepper.txt    folder/salt.txt    userA/    userA/userA.txt    userAfile.txt    userB/    userB/userB.txt    zeroByteFile    zeroByteFolder
@{ICEBERG_PREFIX_USERA_OBS}             userA/userA.txt
@{ICEBERG_PREFIX_USERA_FSO}             userA/    userA/userA.txt
@{ICEBERG_PREFIX_USERA_STAR_OBS}        userA/userA.txt    userAfile.txt
@{ICEBERG_PREFIX_USERA_STAR_FSO}        userA/    userA/userA.txt
@{ICEBERG_PREFIX_USER_OBS}              userA/userA.txt    userB/userB.txt    userAfile.txt
@{ICEBERG_PREFIX_USER_FSO}              userA/    userA/userA.txt    userB/    userB/userB.txt    userAfile.txt

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
    ${baseline_no_prefix_no_delimiter} =              List Object Keys Should Succeed  ${bucket}  ${api}  profile=permanent
    ${baseline_userA_slash_prefix_no_delimiter} =     List Object Keys Should Succeed  ${bucket}  ${api}  userA/  profile=permanent
    ${baseline_userA_prefix_no_delimiter} =           List Object Keys Should Succeed  ${bucket}  ${api}  userA  profile=permanent
    ${baseline_no_prefix_slash_delimiter} =           List Object Keys Should Succeed  ${bucket}  ${api}  profile=permanent  delimiter=/
    ${baseline_userA_slash_prefix_slash_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA/  profile=permanent  delimiter=/
    ${baseline_userA_prefix_slash_delimiter} =        List Object Keys Should Succeed  ${bucket}  ${api}  userA  profile=permanent  delimiter=/
    ${baseline_userA_userA_prefix_no_delimiter} =     List Object Keys Should Succeed  ${bucket}  ${api}  userA/userA  profile=permanent
    ${baseline_userA_userA_prefix_slash_delimiter} =  List Object Keys Should Succeed  ${bucket}  ${api}  userA/userA  profile=permanent  delimiter=/
    ${baseline_user_prefix_no_delimiter} =            List Object Keys Should Succeed  ${bucket}  ${api}  user  profile=permanent
    ${baseline_user_prefix_slash_delimiter} =         List Object Keys Should Succeed  ${bucket}  ${api}  user  profile=permanent  delimiter=/
    IF                            '${bucket}' == '${ICEBERG_BUCKET_OBS}'
        Assert Listed Keys Should Equal  ${baseline_no_prefix_no_delimiter}  @{ICEBERG_LISTABLE_OBJECT_KEYS_OBS}
        Assert Listed Keys Should Equal  ${baseline_userA_slash_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_OBS}
        Assert Listed Keys Should Equal  ${baseline_userA_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_STAR_OBS}
        Assert Listed Keys Should Equal  ${baseline_user_prefix_no_delimiter}  @{ICEBERG_PREFIX_USER_OBS}
    ELSE IF                       '${bucket}' == '${ICEBERG_BUCKET_FSO}'
        Assert Listed Keys Should Equal  ${baseline_no_prefix_no_delimiter}  @{ICEBERG_LISTABLE_OBJECT_KEYS_FSO}
        Assert Listed Keys Should Equal  ${baseline_userA_slash_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_FSO}
        Assert Listed Keys Should Equal  ${baseline_userA_prefix_no_delimiter}  @{ICEBERG_PREFIX_USERA_STAR_FSO}
        Assert Listed Keys Should Equal  ${baseline_user_prefix_no_delimiter}  @{ICEBERG_PREFIX_USER_FSO}
    END

    # a) IAM session policy with no s3:prefix condition.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_no_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}

    # b1) StringEquals without wildcard.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":"userA/"}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    # If a prefix was authorized in the session policy, but the user did not supply any prefix, verify access denied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    # If a prefix was authorized in the session policy, but the user supplied a different prefix, verify access denied (userA does not match userA/)
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA

    # b2) StringEquals with wildcard only, wildcard prefix should be ignored and deny.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":"userA/*"}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA/  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # b3) StringEquals mixed values, wildcard entry is ignored but exact entry still works.  Ranger doesn't support literal asterisk matching
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":["userA/","userA/*"]}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_slash_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # b4) StringEquals without wildcard, deeper prefix with delimiter.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringEquals":{"s3:prefix":"userA/userA"}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/userA  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_userA_prefix_slash_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  userA/userA
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_userA_userA_prefix_no_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # c1) StringLike without wildcard.
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringLike":{"s3:prefix":"userA/"}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
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
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
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
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
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

    # c4) StringLike with prefix only (user edge case).
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${bucket}","Condition":{"StringLike":{"s3:prefix":"user"}}}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  user
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_user_prefix_no_delimiter}
    ${keys_json} =                List Object Keys Should Succeed  ${bucket}  ${api}  user  delimiter=/
    Assert Listed Keys Json Should Equal  ${keys_json}  ${baseline_user_prefix_slash_delimiter}
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  delimiter=/
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA
    List Object Keys Should Fail  ${bucket}  ${api}  AccessDenied  userA  delimiter=/

    # d) No IAM session policy (it should work just like the baseline)
    Assume Role And Configure STS Profile                       perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
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

Configure STS Profile With Bogus Credential Part
    [Arguments]                   ${bogus_part}
    IF                            '${bogus_part}' == 'accessKeyId'
        Configure STS Profile     bogusAccessKeyId  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ELSE IF                       '${bogus_part}' == 'secretKey'
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  bogusSecretKey  ${STS_SESSION_TOKEN}
    ELSE IF                       '${bogus_part}' == 'sessionToken'
        Configure STS Profile     ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  bogusSessionToken
    END

*** Test Cases ***
Create User in Ranger
    ${user_json} =                Set Variable                  { "loginId": "${ICEBERG_SVC_CATALOG_USER}", "name": "${ICEBERG_SVC_CATALOG_USER}", "password": "Password123", "firstName": "Iceberg REST", "lastName": "Catalog", "emailAddress": "${ICEBERG_SVC_CATALOG_USER}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }
    Create Ranger User            ${user_json}

Create All Access Roles in Ranger
    FOR    ${role}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_MULTI_BUCKET_ROLE}
        ${role_json} =            Set Variable                  { "name": "${role}", "description": "Iceberg data all access" }
        Create Ranger Role        ${role_json}
    END

Create Read Only Roles in Ranger
    FOR    ${role}    IN    ${ICEBERG_READ_ONLY_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${role_json} =            Set Variable                  { "name": "${role}", "description": "Iceberg data read only" }
        Create Ranger Role        ${role_json}
    END

Create Temp Bucket Access Policies
    # ${STS_TEMP_BUCKET_ROLE} keeps the sts-bucket-* namespace available for the create/delete bucket tests.
    ${role_json} =                Set Variable                  { "name": "${STS_TEMP_BUCKET_ROLE}", "description": "STS temp bucket access" }
    Create Ranger Role            ${role_json}

    Create Ranger Assume Role Policy  ${STS_TEMP_BUCKET_ROLE}   ${ICEBERG_SVC_CATALOG_USER}

    ${bucket_policy} =            Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "sts temp bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "sts-bucket-*" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${STS_TEMP_BUCKET_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${ICEBERG_READ_ONLY_ROLE_OBS}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${bucket_policy}

Create All Access Assume Role Policies
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on each all-access role.
    FOR    ${role}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_MULTI_BUCKET_ROLE}
        Create Ranger Assume Role Policy  ${role}               ${ICEBERG_SVC_CATALOG_USER}
    END

Create Read Only Assume Role Policies
    # This policy gives '${ICEBERG_SVC_CATALOG_USER}' user ASSUME_ROLE permission on each read-only role.
    FOR    ${role}    IN    ${ICEBERG_READ_ONLY_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        Create Ranger Assume Role Policy  ${role}               ${ICEBERG_SVC_CATALOG_USER}
    END

Create Iceberg Volume Access Policy
    # This policy gives all iceberg roles READ,LIST permission on volume s3v.
    # It also gives '${ICEBERG_SVC_CATALOG_USER}' user READ permission on volume s3v.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg volume access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${ICEBERG_ALL_ACCESS_ROLE_OBS}", "${ICEBERG_ALL_ACCESS_ROLE_FSO}", "${ICEBERG_MULTI_BUCKET_ROLE}", "${STS_TEMP_BUCKET_ROLE}", "${ICEBERG_READ_ONLY_ROLE_OBS}", "${ICEBERG_READ_ONLY_ROLE_FSO}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Bucket Access Policies
    # This loop gives '${ICEBERG_ALL_ACCESS_ROLE_OBS}' ALL permission on '${ICEBERG_BUCKET_OBS}', '${ICEBERG_SVC_CATALOG_USER}' user READ, LIST permission on '${ICEBERG_BUCKET_OBS}' [because hdfs user creates the buckets, we need READ, LIST to get the baseline results], '${ICEBERG_READ_ONLY_ROLE_OBS}' READ permission on '${ICEBERG_BUCKET_OBS}'
    # It also gives '${ICEBERG_ALL_ACCESS_ROLE_FSO}' ALL permission on '${ICEBERG_BUCKET_FSO}', ${ICEBERG_SVC_CATALOG_USER}' user READ, LIST permission on '${ICEBERG_BUCKET_FSO}' [because hdfs user creates the buckets, we need READ, LIST to get the baseline results], '${ICEBERG_READ_ONLY_ROLE_FSO}' READ permission on '${ICEBERG_BUCKET_FSO}'
    FOR    ${bucket}    ${all_access_role}    ${read_only_role}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg ${bucket} bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${bucket}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${all_access_role}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${read_only_role}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Create Iceberg Table Access Policies
    # This loop gives '${ICEBERG_ALL_ACCESS_ROLE_OBS}' ALL permission on '${ICEBERG_BUCKET_OBS}'/* keys, '${ICEBERG_SVC_CATALOG_USER}' user READ permission on '${ICEBERG_BUCKET_OBS}'/* keys [because hdfs user creates the buckets, we need READ to get the baseline results], '${ICEBERG_READ_ONLY_ROLE_OBS}' READ permission on '${ICEBERG_BUCKET_OBS}'/* keys
    # It also gives '${ICEBERG_ALL_ACCESS_ROLE_FSO}' ALL permission on '${ICEBERG_BUCKET_FSO}'/* keys, '${ICEBERG_SVC_CATALOG_USER}' user READ permission on '${ICEBERG_BUCKET_FSO}'/* keys [because hdfs user creates the buckets, we need READ to get the baseline results], '${ICEBERG_READ_ONLY_ROLE_FSO}' READ permission on '${ICEBERG_BUCKET_FSO}'/* keys
    FOR    ${bucket}    ${all_access_role}    ${read_only_role}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS}    ${ICEBERG_READ_ONLY_ROLE_OBS}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO}    ${ICEBERG_READ_ONLY_ROLE_FSO}
        ${policy_json} =          Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg ${bucket} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${bucket}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${all_access_role}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${ICEBERG_SVC_CATALOG_USER}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${read_only_role}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
        Create Ranger Policy      ${policy_json}
    END

Create Iceberg Multi-Bucket Role Policies
    ${bucket_policy} =            Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg multi bucket access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET_OBS}", "${ICEBERG_BUCKET_FSO}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_MULTI_BUCKET_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${bucket_policy}
    ${key_policy} =               Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg multi table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET_OBS}", "${ICEBERG_BUCKET_FSO}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${ICEBERG_MULTI_BUCKET_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${key_policy}

Create Partial Access Roles in Ranger
    FOR    ${role}    IN    ${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE}    ${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE}    ${PARTIAL_BUCKET_READ_ROLE}    ${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}    ${PARTIAL_BUCKET_LIST_ROLE}    ${PARTIAL_BUCKET_READ_ACL_ROLE}    ${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}    ${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}
        ${role_json} =            Set Variable                  { "name": "${role}", "description": "Partial access role" }
        Create Ranger Role        ${role_json}
    END

Create Partial Access Assume Role Policies
    FOR    ${role}    IN    ${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE}    ${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE}    ${PARTIAL_BUCKET_READ_ROLE}    ${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}    ${PARTIAL_BUCKET_LIST_ROLE}    ${PARTIAL_BUCKET_READ_ACL_ROLE}    ${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}    ${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}
        Create Ranger Assume Role Policy  ${role}              ${ICEBERG_SVC_CATALOG_USER}
    END

Create Partial Access Volume Policies
    # Append partial-role items to existing "iceberg volume access" policy to avoid duplicate resourceSignature conflicts.
    ${policy_items} =             Set Variable                  [ { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "list", "isAllowed": true } ], "roles": [ "${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_ROLE}", "${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}", "${PARTIAL_BUCKET_LIST_ROLE}", "${PARTIAL_BUCKET_READ_ACL_ROLE}", "${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}", "${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}" ], "delegateAdmin": false } ]
    Update Ranger Policy Items    iceberg volume access         ${policy_items}

Create Partial Access Bucket Policies
    # Append partial-role items to existing "iceberg ${ICEBERG_BUCKET_OBS} bucket access" policy.
    ${policy_items} =             Set Variable                  [ { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "list", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_LIST_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read_acl", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_ACL_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}", "${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}" ], "delegateAdmin": false } ]
    Update Ranger Policy Items    iceberg ${ICEBERG_BUCKET_OBS} bucket access  ${policy_items}

Create Partial Access Table Policies
    # Append partial-role items to existing "iceberg ${ICEBERG_BUCKET_OBS} table access" policy.
    ${policy_items} =             Set Variable                  [ { "accesses": [ { "type": "list", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_ROLE}", "${PARTIAL_BUCKET_LIST_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "create", "isAllowed": true } ], "roles": [ "${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "write", "isAllowed": true } ], "roles": [ "${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE}" ], "delegateAdmin": false } ]
    Update Ranger Policy Items    iceberg ${ICEBERG_BUCKET_OBS} table access  ${policy_items}
    # One-off policy for Negative A in "STS session policy ListBucket must require bucket READ and LIST":
    # Grant LIST on key prefix "upload*" (not "*") so it doesn't implicitly satisfy bucket-level LIST in Ranger matching.
    ${upload_prefix_policy} =      Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "iceberg ${ICEBERG_BUCKET_OBS} upload prefix list", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "s3v" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${ICEBERG_BUCKET_OBS}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "upload*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "list", "isAllowed": true } ], "roles": [ "${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${upload_prefix_policy}

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

    # Create buckets as a different user so the permanent credential principal isn't the bucket owner.
    # Otherwise Ranger's default owner privileges can mask missing READ/LIST permissions in STS tests.
    # Populate the buckets as this user as well.
    Kinit test user               hdfs                          hdfs.keytab
    Execute                       ozone sh bucket create --layout ${ICEBERG_LAYOUT_OBS} /s3v/${ICEBERG_BUCKET_OBS}
    Execute                       ozone sh bucket create --layout ${ICEBERG_LAYOUT_FSO} /s3v/${ICEBERG_BUCKET_FSO}
    Populate Iceberg Bucket       ${ICEBERG_BUCKET_OBS}
    Populate Iceberg Bucket       ${ICEBERG_BUCKET_FSO}

    # Switch back to the service catalog principal for running S3/STS requests.
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

Assume Role for Limited-Scope Token
    # All access role is limited to read-only via session policy
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role for Role-Scoped Token
    # Create token with full permissions of all access role
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role And Configure STS Profile                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        ${bucketSuffix} =         Generate Random String        8   [LOWER]
        ${tmp_bucket} =           Set Variable                  sts-bucket-${bucketSuffix}
        Create Bucket Should Fail  ${tmp_bucket}
        Get Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
        Put Object Should Succeed  ${bucket}  ${ICEBERG_BUCKET_TESTFILE}
    END

Assume Role with Invalid Action in Session Policy
    # s3:InvalidAction in the session policy is not valid => no access is given to the token
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:InvalidAction","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Mismatched Action and Resource in Session Policy
    # s3:GetObject is for object resources but the Resource "arn:aws:s3:::bucket" is a bucket resource => no access is given to the token
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}"}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Elevated Access in Session Policy Should Fail
    # Assume read-only role but try to grant write access in session policy - this should not be allowed => no access given to the token
    FOR    ${bucket}    ${read_only_role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${READ_ONLY_ROLE_OBS_ARN} 
    ...    ${ICEBERG_BUCKET_FSO}    ${READ_ONLY_ROLE_FSO_ARN} 
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${read_only_role_arn}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with Malformed Session Policy JSON Should Fail
    ${session_policy} =           Set Variable                  {"ThisIsMalformed"}
    FOR    ${role_arn}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  policy_json=${session_policy}  expected_error=MalformedPolicyDocument  expected_http_code=400  role_arn=${role_arn}
    END

Assume Role with Unsupported Condition Operator in Session Policy Should Fail
    # StringNotEqualsIgnoreCase is currently unsupported
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*","Condition":{"StringNotEqualsIgnoreCase":{"s3:prefix":"my_table/*"}}}]}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  policy_json=${session_policy}  expected_error=UnsupportedOperation  expected_http_code=501  role_arn=${role_arn}
    END

Assume Role with GetObject in Session Policy Using s3:prefix Should Fail
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*","Condition":{"StringLike":{"s3:prefix":"file1*"}}}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Assume Role with GetObject in Session Policy Should Not Allow List Buckets
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        ${session_policy} =       Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${bucket}/*"}]}
        Assume Role And Configure STS Profile                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        List Object Keys Should Fail  ${bucket}  list-objects  AccessDenied
        List Object Keys Should Fail  ${bucket}  list-objects-v2  AccessDenied
    END

STS Token with Bogus Credential Part Should Fail
    # If two of the three STS credential components are valid and one is bogus, the token must not give any access
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role And Get Temporary Credentials               perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        FOR    ${bogus_part}    IN    accessKeyId    secretKey    sessionToken
            Configure STS Profile With Bogus Credential Part     ${bogus_part}
            ${bucketSuffix} =     Generate Random String         8   [LOWER]
            ${tmp_bucket} =       Set Variable                   sts-bucket-${bucketSuffix}
            Create Bucket Should Fail  ${tmp_bucket}
            Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
            Put Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
        END
    END

Assume Role Without Ranger Permission Should Fail
    # A user who doesn't have assume role permission in Ranger should not be able to invoke the api
    Kinit test user               ${TEST_USER_NON_ADMIN}        ${TEST_USER_NON_ADMIN}.keytab

    ${output} =                   Execute                       ozone s3 getsecret
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=awsAccessKey=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=awsSecret=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}

    FOR    ${role_arn}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role Should Fail   perm_access_key_id=${accessKeyId}  perm_secret_key=${secretKey}  expected_error=AccessDenied  role_arn=${role_arn}
    END

Assume Role With Incorrect Permanent Credentials Should Fail
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   ${ICEBERG_SVC_CATALOG_USER}.keytab

    FOR    ${role_arn}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role Should Fail   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=InvalidSecretKey  expected_error=InvalidClientTokenId  role_arn=${role_arn}
    END

STS Token with Presigned URL
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role And Configure STS Profile                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        ${presigned_url} =        Execute                       aws s3 presign s3://${bucket}/${ICEBERG_BUCKET_TESTFILE} --endpoint-url ${S3G_ENDPOINT_URL} --profile sts
        Should Contain            ${presigned_url}              X-Amz-Algorithm=AWS4-HMAC-SHA256
        Should Contain            ${presigned_url}              X-Amz-Security-Token=
        ${output} =               Execute                       curl -v '${presigned_url}'
        Should Contain            ${output}                     HTTP/1.1 200 OK
    END

Verify Token Revocation via CLI
    FOR    ${bucket}    ${role_arn}    IN
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        Assume Role And Configure STS Profile                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${role_arn}
        ${output} =               Execute                       ozone s3 revokeststoken -t ${STS_SESSION_TOKEN} -y ${OM_HA_PARAM}
        Should Contain            ${output}                     STS token revoked for sessionToken
        # Trying to use the token for even get-object should now fail.
        Get Object Should Fail    ${bucket}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    END

Non-Admin Cannot Revoke STS Token
    FOR    ${role_arn}    IN    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
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
    ...    ${ICEBERG_BUCKET_OBS}    ${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ...    ${ICEBERG_BUCKET_FSO}    ${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
        FOR    ${api}    IN    list-objects    list-objects-v2
            Run List Prefix And Delimiter Policy Matrix For Bucket And Api    ${bucket}  ${role_arn}  ${api}
        END
    END

Assume Role Request With Oversized Payload Should Fail
    ${large_policy} =             Generate Oversized Session Policy
    Assume Role Should Fail       perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  policy_json=${large_policy}  expected_error=PayloadTooLarge  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}

Tampered STS Token Service, Policy, or Signature Must Fail
    # Taking valid STS session token and mutating different parts of it must render it unusable
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${token_with_service_tamper} =  Mutate STS Session Token    ${STS_SESSION_TOKEN}  service
    Configure STS Profile                                       ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${token_with_service_tamper}
    Get Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

    ${token_with_policy_tamper} =  Mutate STS Session Token     ${STS_SESSION_TOKEN}  session_policy
    Configure STS Profile                                       ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${token_with_policy_tamper}
    Get Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

    ${token_with_signature_tamper} =  Mutate STS Session Token  ${STS_SESSION_TOKEN}  signature
    Configure STS Profile                                       ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${token_with_signature_tamper}
    Get Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail      ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

Assume Role Session Policy With Multiple Buckets Should Access All Buckets
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/*"},{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_FSO}/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_MULTI_BUCKET_ROLE_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Succeed     ${ICEBERG_BUCKET_FSO}  ${ICEBERG_BUCKET_TESTFILE}
    Put Object Should Fail        ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail        ${ICEBERG_BUCKET_FSO}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

Assume Role Session Policy With Wildcard Bucket Should Work
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::iceberg-*/*"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_MULTI_BUCKET_ROLE_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Succeed     ${ICEBERG_BUCKET_FSO}  ${ICEBERG_BUCKET_TESTFILE}
    Put Object Should Fail        ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

Assume Role Without Duration Should Default To One Hour
    # The Assume Role and Get Temporary Credentials keyword has a check for default expiration of 3600 seconds (i.e. one hour) if duration is not supplied, which it is not supplied here
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}  duration_seconds=${EMPTY}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}

Assume Role Should Fail For Too Short Role Arn
    Assume Role Should Fail Using Curl  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  expected_error=ValidationError  expected_http_code=400  role_arn=a

Assume Role Should Fail For Too Short Role Session Name
    Assume Role Should Fail Using Curl  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  expected_error=ValidationError  expected_http_code=400  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}  role_session_name=a

Assume Role Response Should Include STS Request Headers
    Assume Role Response Headers Should Be Present              ${PERMANENT_ACCESS_KEY_ID}  ${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}

STS session policy ListAllMyBuckets must require volume READ and LIST
    # Positive control
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-buckets --profile sts
    Should Contain                ${output}                     ${ICEBERG_BUCKET_OBS}

    # Negative A: missing LIST
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_LIST_ALL_BUCKETS_VOL_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-buckets --profile sts
    Should Contain                ${output}                     AccessDenied

    # Negative B: missing READ
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_LIST_ALL_BUCKETS_VOL_LIST_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-buckets --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy ListBucket must require bucket READ and LIST
    # Positive control
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    List Object Keys Should Succeed  ${ICEBERG_BUCKET_OBS}

    # Negative A: missing LIST
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_UPLOAD_PREFIX_ROLE_ARN}
    List Object Keys Should Fail  ${ICEBERG_BUCKET_OBS}  list-objects  AccessDenied  upload

    # Negative B: missing READ
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_LIST_ROLE_ARN}
    List Object Keys Should Fail  ${ICEBERG_BUCKET_OBS}  list-objects  AccessDenied

STS session policy ListBucketMultipartUploads must require bucket READ and LIST
    # Positive control
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucketMultipartUploads","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-multipart-uploads --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Not Contain            ${output}                     AccessDenied

    # Negative A: missing LIST
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${READ_ONLY_ROLE_OBS_ARN} 
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-multipart-uploads --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Negative B: missing READ
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_LIST_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-multipart-uploads --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy GetBucketAcl must require bucket READ and READ_ACL
    # Positive control
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-bucket-acl --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Contain                ${output}                     Owner

    # Negative A: missing READ_ACL
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-bucket-acl --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Negative B: missing READ
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ACL_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-bucket-acl --bucket ${ICEBERG_BUCKET_OBS} --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy PutObject must require key CREATE and WRITE
    # Positive control
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/*"}]}
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Put Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}

    # Negative A: missing WRITE
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_PUT_OBJECT_KEY_CREATE_ROLE_ARN}
    Put Object Should Fail        ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

    # Negative B: missing CREATE
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_PUT_OBJECT_KEY_WRITE_ROLE_ARN}
    Put Object Should Fail        ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

STS session policy CreateBucket must require bucket CREATE
    ${bucket_suffix} =            Generate Random String        8   [LOWER]
    ${bucket} =                   Set Variable                  sts-bucket-${bucket_suffix}
    ${create_policy} =            Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:CreateBucket","Resource":"arn:aws:s3:::${bucket}"}]}
    ${delete_policy} =            Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:DeleteBucket","Resource":"arn:aws:s3:::${bucket}"}]}

    # Negative: missing CREATE
    Assume Role And Configure STS Profile                       policy_json=${create_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${READ_ONLY_ROLE_OBS_ARN} 
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket ${bucket} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${create_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${STS_TEMP_BUCKET_ROLE_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket ${bucket} --profile sts
    Should Contain                ${output}                     Location
    Should Contain                ${output}                     ${bucket}

    # Cleanup
    Assume Role And Configure STS Profile                       policy_json=${delete_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${STS_TEMP_BUCKET_ROLE_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket ${bucket} --profile sts
    Should Not Contain            ${output}                     AccessDenied

STS session policy DeleteBucket must require bucket DELETE
    ${bucket_suffix} =            Generate Random String        8   [LOWER]
    ${bucket} =                   Set Variable                  sts-bucket-${bucket_suffix}
    ${delete_policy} =            Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:DeleteBucket","Resource":"arn:aws:s3:::${bucket}"}]}

    # Prepare the bucket as a different principal so bucket-owner privileges
    # do not mask missing DELETE permissions in the negative case.
    Kinit test user               hdfs                          hdfs.keytab
    ${output} =                   Execute                       ozone sh bucket create --layout ${ICEBERG_LAYOUT_OBS} /s3v/${bucket}
    Kinit test user               ${ICEBERG_SVC_CATALOG_USER}   svc-iceberg-rest-catalog.keytab

    # Negative: missing DELETE
    Assume Role And Configure STS Profile                       policy_json=${delete_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${READ_ONLY_ROLE_OBS_ARN} 
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket ${bucket} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${delete_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${STS_TEMP_BUCKET_ROLE_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-bucket --bucket ${bucket} --profile sts
    Should Not Contain            ${output}                     AccessDenied

STS session policy PutBucketAcl must require bucket WRITE_ACL
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}"}]}

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-bucket-acl --bucket ${ICEBERG_BUCKET_OBS} --grant-read "" --profile sts
    Should Not Contain            ${output}                     AccessDenied

    # Negative: missing WRITE_ACL
    Assume Role And Configure STS Profile                       policy_json=${session_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-bucket-acl --bucket ${ICEBERG_BUCKET_OBS} --grant-read "" --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy GetObjectTagging must require key READ
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-object-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 get-object-tagging content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${get_tag_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Put Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${key}  ${local_path}

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${get_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     TagSet
    ${tag_count} =                Execute                       echo '${output}' | jq -r '.TagSet | length'
    Should Be Equal As Strings    ${tag_count}                  0

    # Negative: missing READ - ${PARTIAL_BUCKET_READ_ROLE_ARN} has LIST permission on the key
    Assume Role And Get Temporary Credentials                   policy_json=${get_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy PutObjectTagging must require key WRITE
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-object-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 put-object-tagging content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${get_tag_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${put_tag_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Put Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${key}  ${local_path}

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${put_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --tagging '{"TagSet":[{"Key":"tag-key1","Value":"tag-value1"}]}' --profile sts
    Should Not Contain            ${output}                     AccessDenied

    # Read tags with a session that allows GetObjectTagging
    Assume Role And Configure STS Profile                       policy_json=${get_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     TagSet
    ${tag_count} =                Execute                       echo '${output}' | jq -r '.TagSet | length'
    Should Be Equal As Strings    ${tag_count}                  1

    # Negative: missing WRITE
    Assume Role And Get Temporary Credentials                   policy_json=${put_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --tagging '{"TagSet":[{"Key":"tag-key2","Value":"tag-value2"}]}' --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy DeleteObjectTagging must require key WRITE
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-object-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 delete-object-tagging content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${put_tag_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${delete_tag_policy} =        Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:DeleteObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Put Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${key}  ${local_path}

    # Seed a tag so the delete actually has something to remove.
    Assume Role And Configure STS Profile                       policy_json=${put_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --tagging '{"TagSet":[{"Key":"tag-key1","Value":"tag-value1"}]}' --profile sts
    Should Not Contain            ${output}                     AccessDenied

    # Negative: missing WRITE
    Assume Role And Configure STS Profile                       policy_json=${delete_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${delete_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Not Contain            ${output}                     AccessDenied

    # Read tags with a session that allows GetObjectTagging
    ${get_tag_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    Assume Role And Get Temporary Credentials                   policy_json=${get_tag_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object-tagging --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     TagSet
    ${tag_count} =                Execute                       echo '${output}' | jq -r '.TagSet | length'
    Should Be Equal As Strings    ${tag_count}                  0

STS session policy DeleteObject must require key DELETE
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-object-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 delete-object content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${delete_object_policy} =     Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Put Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${key}  ${local_path}

    # Negative: missing DELETE
    Assume Role And Configure STS Profile                       policy_json=${delete_object_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-object --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${delete_object_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} delete-object --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    Should Not Contain            ${output}                     AccessDenied

STS session policy ListMultipartUploadParts must require key LIST
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-mpu-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 list multipart upload content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${list_parts_policy} =        Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListMultipartUploadParts","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-multipart-upload --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    ${upload_id} =                Execute                       echo '${output}' | jq -r '.UploadId'
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} upload-part --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --part-number 1 --body ${local_path} --upload-id ${upload_id} --profile sts
    Should Contain                ${output}                     ETag

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${list_parts_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-parts --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --upload-id ${upload_id} --profile sts
    Should Contain                ${output}                     PartNumber

    # Negative: missing LIST
    Assume Role And Configure STS Profile                       policy_json=${list_parts_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${READ_ONLY_ROLE_OBS_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} list-parts --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --upload-id ${upload_id} --profile sts
    Should Contain                ${output}                     AccessDenied

STS session policy AbortMultipartUpload must require key WRITE
    ${key_suffix} =               Generate Random String        8   [LOWER]
    ${key} =                      Set Variable                  sts-mpu-${key_suffix}.txt
    ${local_path} =               Set Variable                  ${TEMP_DIR}/${key}
    Create File                   ${local_path}                 abort multipart upload content
    ${put_policy} =               Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}
    ${abort_policy} =             Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:AbortMultipartUpload","Resource":"arn:aws:s3:::${ICEBERG_BUCKET_OBS}/${key}"}]}

    Assume Role And Configure STS Profile                       policy_json=${put_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-multipart-upload --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --profile sts
    ${upload_id} =                Execute                       echo '${output}' | jq -r '.UploadId'
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} upload-part --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --part-number 1 --body ${local_path} --upload-id ${upload_id} --profile sts
    Should Contain                ${output}                     ETag

    # Negative: missing WRITE
    Assume Role And Configure STS Profile                       policy_json=${abort_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${PARTIAL_BUCKET_READ_ROLE_ARN}
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} abort-multipart-upload --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --upload-id ${upload_id} --profile sts
    Should Contain                ${output}                     AccessDenied

    # Positive control
    Assume Role And Configure STS Profile                       policy_json=${abort_policy}  perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    ${output} =                   Execute                       aws s3api --endpoint-url ${S3G_ENDPOINT_URL} abort-multipart-upload --bucket ${ICEBERG_BUCKET_OBS} --key ${key} --upload-id ${upload_id} --profile sts
    Should Not Contain            ${output}                     AccessDenied

Revoking Permanent User Must Revoke Existing Session Token
    # Create session tokens for both buckets, verify they work, then revoke permanent user secret and verify both fail.
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_OBS_ARN}
    Set Test Variable             ${OBS_STS_ACCESS_KEY_ID}      ${STS_ACCESS_KEY_ID}
    Set Test Variable             ${OBS_STS_SECRET_KEY}         ${STS_SECRET_KEY}
    Set Test Variable             ${OBS_STS_SESSION_TOKEN}      ${STS_SESSION_TOKEN}
    Configure STS Profile         ${OBS_STS_ACCESS_KEY_ID}  ${OBS_STS_SECRET_KEY}  ${OBS_STS_SESSION_TOKEN}
    Get Object Should Succeed     ${ICEBERG_BUCKET_OBS}  ${ICEBERG_BUCKET_TESTFILE}

    Assume Role And Get Temporary Credentials                   perm_access_key_id=${PERMANENT_ACCESS_KEY_ID}  perm_secret_key=${PERMANENT_SECRET_KEY}  role_arn=${ICEBERG_ALL_ACCESS_ROLE_FSO_ARN}
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
