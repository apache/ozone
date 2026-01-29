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
Documentation       Smoke test for S3 STS AssumeRole + Temp Creds (Multi-Tenant Scenario)
Resource            ./ozone-secure-sts.resource
Resource            ../admincli/lib.resource
Test Timeout        10 minutes

*** Variables ***
${TENANT_ONE}                     sts-tenant-one
${TENANT_TWO}                     sts-tenant-two
${TENANT_THREE}                   sts-tenant-three
${USER_A}                         svc-iceberg-userA
${USER_B}                         svc-iceberg-userB
${TENANT_ONE_ROLE}                sts-tenant-one-role
${TENANT_TWO_ROLE}                sts-tenant-two-role
${TENANT_THREE_ROLE}              sts-tenant-three-role
${TENANT_ONE_ICEBERG_BUCKET}      iceberg-tenant-one-bucket
${TENANT_TWO_ICEBERG_BUCKET}      iceberg-tenant-two-bucket
${TENANT_THREE_ICEBERG_BUCKET}    iceberg-tenant-three-bucket
${TENANT_TWO_ANOTHER_BUCKET}      tenant-two-another-bucket
${TENANT_ONE_LINKED_BUCKET}       tenant-one-a-linked-bucket
${ICEBERG_BUCKET_TESTFILE}        testfile23
${ANOTHER_BUCKET_TESTFILE}        testfile00
${OM_ADMIN_USER}                  hdfs

*** Keywords ***
Update Resource Policy
    [Arguments]                   ${policy_name}  ${tenant_id}  ${policy_item_json}
    # Fetch the existing policy json created from the ozone tenant commands
    ${policy} =                   Execute                       curl --fail --silent --show-error --location --netrc --request GET --header "accept: application/json" "${RANGER_ENDPOINT_URL}/service/public/v2/api/service/dev_ozone/policy/${policy_name}"
    # Get policy id, then update by id
    ${policy_id} =                Execute                       printf '%s' '${policy}' | jq -r '.id'
    # Update the policyItem with the supplied ${policy_item_json}
    ${updated} =                  Execute                       printf '%s' '${policy}' | jq '.policyItems += ${policy_item_json}'
    ${result} =                   Execute                       curl --fail --include --location --netrc -X PUT -H "Content-Type: application/json" -H "accept: application/json" --data '${updated}' "${RANGER_ENDPOINT_URL}/service/public/v2/api/policy/${policy_id}"
    Should Contain                ${result}                     HTTP/1.1 200

*** Test Cases ***
Create Users in Ranger
    ${user_json} =                Set Variable                  { "loginId": "${USER_A}", "name": "${USER_A}", "password": "Password123", "firstName": "User A Iceberg REST", "lastName": "Catalog", "emailAddress": "${USER_A}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }
    Create Ranger User            ${user_json}
    ${user_json} =                Set Variable                  { "loginId": "${USER_B}", "name": "${USER_B}", "password": "Password123", "firstName": "User B Iceberg REST", "lastName": "Catalog", "emailAddress": "${USER_B}@example.com", "userRoleList": ["ROLE_USER"], "userPermList": [ { "moduleId": 1, "isAllowed": 1 }, { "moduleId": 3, "isAllowed": 1 }, { "moduleId": 7, "isAllowed": 1 } ] }
    Create Ranger User            ${user_json}

Create Tenants
    Kinit test user               ${OM_ADMIN_USER}              ${OM_ADMIN_USER}.keytab
    ${output} =                   Execute                       ozone tenant --verbose create ${TENANT_ONE}
    Should contain                ${output}                     "tenantId" : "${TENANT_ONE}"
    ${output} =                   Execute                       ozone tenant --verbose create ${TENANT_TWO}
    Should contain                ${output}                     "tenantId" : "${TENANT_TWO}"
    ${output} =                   Execute                       ozone tenant --verbose create ${TENANT_THREE}
    Should contain                ${output}                     "tenantId" : "${TENANT_THREE}"

Assign Users to Tenants
    ${output} =                   Execute                       ozone tenant --verbose user assign ${USER_A} --tenant=${TENANT_ONE}
    Should contain                ${output}                     Assigned '${USER_A}' to '${TENANT_ONE}'
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=export AWS_ACCESS_KEY_ID=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=export AWS_SECRET_ACCESS_KEY=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}
    Set Global Variable           ${USER_A_T1_PERM_ACCESS_KEY_ID}  ${accessKeyId}
    Set Global Variable           ${USER_A_T1_PERM_SECRET_KEY}  ${secretKey}

    ${output} =                   Execute                       ozone tenant --verbose user assign ${USER_A} --tenant=${TENANT_TWO}
    Should contain                ${output}                     Assigned '${USER_A}' to '${TENANT_TWO}'
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=export AWS_ACCESS_KEY_ID=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=export AWS_SECRET_ACCESS_KEY=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}
    Set Global Variable           ${USER_A_T2_PERM_ACCESS_KEY_ID}   ${accessKeyId}
    Set Global Variable           ${USER_A_T2_PERM_SECRET_KEY}  ${secretKey}

    ${output} =                   Execute                       ozone tenant --verbose user assign ${USER_B} --tenant=${TENANT_THREE}
    Should contain                ${output}                     Assigned '${USER_B}' to '${TENANT_THREE}'
    ${accessKeyId} =              Get Regexp Matches            ${output}     (?<=export AWS_ACCESS_KEY_ID=).*
    ${secretKey} =                Get Regexp Matches            ${output}     (?<=export AWS_SECRET_ACCESS_KEY=).*
    ${accessKeyId} =              Set Variable                  ${accessKeyId[0]}
    ${secretKey} =                Set Variable                  ${secretKey[0]}
    Set Global Variable           ${USER_B_T3_PERM_ACCESS_KEY_ID}   ${accessKeyId}
    Set Global Variable           ${USER_B_T3_PERM_SECRET_KEY}  ${secretKey}

Create Roles in Ranger
    ${role_json} =                Set Variable                  { "name": "${TENANT_ONE_ROLE}", "description": "Tenant One Role" }
    Create Ranger Role            ${role_json}
    ${role_json} =                Set Variable                  { "name": "${TENANT_TWO_ROLE}", "description": "Tenant Two Role" }
    Create Ranger Role            ${role_json}
    ${role_json} =                Set Variable                  { "name": "${TENANT_THREE_ROLE}", "description": "Tenant Three Role" }
    Create Ranger Role            ${role_json}

Create Assume Role Policies
    # This policy gives '${USER_A}' user ASSUME_ROLE permission on role '${TENANT_ONE_ROLE}'
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_ONE_ROLE} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${TENANT_ONE_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${USER_A}' user ASSUME_ROLE permission on role '${TENANT_TWO_ROLE}'
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_TWO_ROLE} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${TENANT_TWO_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${USER_B}' user ASSUME_ROLE permission on role '${TENANT_THREE_ROLE}'
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_THREE_ROLE} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${TENANT_THREE_ROLE}"],"isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${USER_B}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Update Tenant Volume Access policies
    # This policy gives '${TENANT_ONE_ROLE}' role READ,LIST permission on volume ${TENANT_ONE}.
    # It also gives '${USER_A}' user READ permission on volume ${TENANT_ONE}.
    ${policy_item_json} =         Set Variable                  [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ]
    Update Resource Policy        ${TENANT_ONE}-VolumeAccess    ${TENANT_ONE}  ${policy_item_json}

    # This policy gives '${TENANT_TWO_ROLE}' role READ,LIST permission on volume ${TENANT_TWO}.
    # It also gives '${USER_A}' user READ permission on volume ${TENANT_TWO}.
    # It also gives '${TENANT_ONE_ROLE}' role READ permission on volume ${TENANT_TWO} so it can access ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    ${policy_item_json} =         Set Variable                  [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${TENANT_TWO_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false } ]
    Update Resource Policy        ${TENANT_TWO}-VolumeAccess    ${TENANT_TWO}  ${policy_item_json}

    # This policy gives '${TENANT_THREE_ROLE}' role READ,LIST permission on volume ${TENANT_THREE}.
    # It also gives '${USER_B}' user READ permission on volume ${TENANT_THREE}.
    ${policy_item_json} =         Set Variable                  [ { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "list", "isAllowed": true } ], "roles": [ "${TENANT_THREE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true } ], "users": [ "${USER_B}" ], "delegateAdmin": false } ]
    Update Resource Policy        ${TENANT_THREE}-VolumeAccess  ${TENANT_THREE}  ${policy_item_json}

Create Bucket Access policies
    # This policy gives '${TENANT_ONE_ROLE}' role ALL permission on buckets ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    # and ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    # It also gives '${USER_A}' user READ, CREATE permissions on bucket ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_ONE} ${TENANT_ONE_ICEBERG_BUCKET} and ${TENANT_ONE_LINKED_BUCKET} access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_ONE}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_ONE_ICEBERG_BUCKET}", "${TENANT_ONE_LINKED_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_TWO_ROLE}' role ALL permission on bucket ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}.
    # It also gives '${USER_A}' user READ, CREATE permissions on bucket ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_TWO} ${TENANT_TWO_ICEBERG_BUCKET} access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_TWO}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_TWO_ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_TWO_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_THREE_ROLE}' role ALL permission on bucket ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.
    # It also gives '${USER_B}' user READ, CREATE permissions on bucket ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_THREE} ${TENANT_THREE_ICEBERG_BUCKET} access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_THREE}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_THREE_ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_THREE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${USER_B}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_ONE_ROLE}' role ALL permission on bucket ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}.
    # It also gives '${USER_A}' user READ, CREATE permissions on bucket ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_TWO} ${TENANT_TWO_ANOTHER_BUCKET} access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_TWO}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_TWO_ANOTHER_BUCKET}" ], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false }, { "accesses": [ { "type": "read", "isAllowed": true }, { "type": "create", "isAllowed": true } ], "users": [ "${USER_A}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Create Iceberg Bucket and Another Bucket Table Access policies
    # This policy gives '${TENANT_ONE_ROLE}' role ALL permission on keys ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}/*
    # and keys ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}/*.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_ONE} ${TENANT_ONE_ICEBERG_BUCKET} and ${TENANT_ONE_LINKED_BUCKET} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_ONE}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_ONE_ICEBERG_BUCKET}", "${TENANT_ONE_LINKED_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_TWO_ROLE}' role ALL permission on keys ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}/*.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_TWO} ${TENANT_TWO_ICEBERG_BUCKET} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_TWO}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_TWO_ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_TWO_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_THREE_ROLE}' role ALL permission on keys ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}/*.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_THREE} ${TENANT_THREE_ICEBERG_BUCKET} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_THREE}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_THREE_ICEBERG_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_THREE_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # This policy gives '${TENANT_ONE_ROLE}' role ALL permission on keys ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}/*.
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${TENANT_TWO} ${TENANT_TWO_ANOTHER_BUCKET} table access", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "volume": { "values": [ "${TENANT_TWO}" ], "isExcludes": false, "isRecursive": false }, "bucket": { "values": [ "${TENANT_TWO_ANOTHER_BUCKET}" ], "isExcludes": false, "isRecursive": false }, "key": { "values": [ "*" ], "isExcludes": false, "isRecursive": true } }, "policyItems": [ { "accesses": [ { "type": "all", "isAllowed": true } ], "roles": [ "${TENANT_ONE_ROLE}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

    # Update Ranger policy cache
    Kinit test user               ${OM_ADMIN_USER}              ${OM_ADMIN_USER}.keytab
    ${om_param} =                 Get OM Service Param
    ${output} =                   Execute                       ozone admin om updateranger ${om_param}
    Should contain                ${output}                     Operation completed successfully

Get S3 Credentials for Principals, Create Buckets, and Upload Files to Buckets
    Kinit test user               ${USER_A}                     ${USER_A}.keytab

    # Waiting for Ranger policy cache refresh - ${USER_A} needs to be able to read ${TENANT_ONE} volume
    Wait Until Keyword Succeeds    30s    5s    Execute         ozone sh volume info ${TENANT_ONE}

    # Kinit OM_ADMIN_USER that has access to all volumes/buckets per Ranger default policies
    Kinit test user               ${OM_ADMIN_USER}              ${OM_ADMIN_USER}.keytab

    # Tenant 1 data
    Execute                       ozone sh bucket create /${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    Create File                   ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}
    Execute                       ozone sh key put /${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}

    # Tenant 2 data and linked bucket
    Execute                       ozone sh bucket create /${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}
    Execute                       ozone sh key put /${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}
    Execute                       ozone sh bucket create /${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    Create File                   ${TEMP_DIR}/${ANOTHER_BUCKET_TESTFILE}
    Execute                       ozone sh key put /${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}/${ANOTHER_BUCKET_TESTFILE} ${TEMP_DIR}/${ANOTHER_BUCKET_TESTFILE}
    # Link ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET} to ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    Execute                       ozone sh bucket link /${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET} /${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}
    Execute                       ozone sh key put /${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}/${ANOTHER_BUCKET_TESTFILE} ${TEMP_DIR}/${ANOTHER_BUCKET_TESTFILE}

    # Tenant 3 data
    Execute                       ozone sh bucket create /${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}
    Execute                       ozone sh key put /${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}/${ICEBERG_BUCKET_TESTFILE} ${TEMP_DIR}/${ICEBERG_BUCKET_TESTFILE}

Create Role-Scoped Tokens for All Three Tenants
    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_ONE_ROLE}
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${USER_A_T1_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_A_T1_PERM_SECRET_KEY}
    Set Global Variable           ${USER_A_T1_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_A_T1_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_A_T1_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_TWO_ROLE}
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${USER_A_T2_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_A_T2_PERM_SECRET_KEY}
    Set Global Variable           ${USER_A_T2_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_A_T2_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_A_T2_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_THREE_ROLE}
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${USER_B_T3_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_B_T3_PERM_SECRET_KEY}
    Set Global Variable           ${USER_B_T3_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_B_T3_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_B_T3_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

Verify Role-Scoped Token Accesses
    Configure STS Profile         ${USER_A_T1_STS_ACCESS_KEY_ID}  ${USER_A_T1_STS_SECRET_KEY}  ${USER_A_T1_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET} but not ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}
    # nor ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.  Also verify that it can write to ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Succeed     ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}

    # This token should be able to read from/write to ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET} which is linked to
    # ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}.  It should NOT be able to read from/write to ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    # even though the ${TENANT_ONE_ROLE} has access because the permanent credential only has access to ${TENANT_ONE} volume.
    Get Object Should Succeed     ${TENANT_ONE_LINKED_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}
    Put Object Should Succeed     ${TENANT_ONE_LINKED_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket

    Configure STS Profile         ${USER_A_T2_STS_ACCESS_KEY_ID}  ${USER_A_T2_STS_SECRET_KEY}  ${USER_A_T2_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET} but not ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    # nor ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET} nor ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}
    # nor ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    # Also verify that it can write to ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  AccessDenied
    Get Object Should Fail        ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Succeed     ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}

    Configure STS Profile         ${USER_B_T3_STS_ACCESS_KEY_ID}  ${USER_B_T3_STS_SECRET_KEY}  ${USER_B_T3_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET} but not ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    # nor ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET} nor ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    # nor ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    # Also verify that it can write to ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Succeed     ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}

Create Limited-Scoped Tokens for All Three Tenants
    # Limit scope to read-only for keys in ${TENANT_ONE_ICEBERG_BUCKET} (note ${TENANT_ONE_LINKED_BUCKET} is excluded)
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource": "arn:aws:s3:::${TENANT_ONE_ICEBERG_BUCKET}/*" }]}
    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_ONE_ROLE}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${USER_A_T1_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_A_T1_PERM_SECRET_KEY}
    Set Global Variable           ${USER_A_T1_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_A_T1_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_A_T1_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

    # Limit scope to read-only for keys in ${TENANT_TWO_ICEBERG_BUCKET}
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${TENANT_TWO_ICEBERG_BUCKET}/*"}]}
    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_TWO_ROLE}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${USER_A_T2_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_A_T2_PERM_SECRET_KEY}
    Set Global Variable           ${USER_A_T2_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_A_T2_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_A_T2_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

    # Limit scope to read-only for ${TENANT_THREE_ICEBERG_BUCKET} keys
    ${session_policy} =           Set Variable                  {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::${TENANT_THREE_ICEBERG_BUCKET}/*"}]}
    Set Global Variable           ${ROLE_ARN}                   arn:aws:iam::123456789012:role/${TENANT_THREE_ROLE}
    Assume Role And Get Temporary Credentials                   policy_json=${session_policy}  perm_access_key_id=${USER_B_T3_PERM_ACCESS_KEY_ID}  perm_secret_key=${USER_B_T3_PERM_SECRET_KEY}
    Set Global Variable           ${USER_B_T3_STS_ACCESS_KEY_ID}  ${STS_ACCESS_KEY_ID}
    Set Global Variable           ${USER_B_T3_STS_SECRET_KEY}   ${STS_SECRET_KEY}
    Set Global Variable           ${USER_B_T3_STS_SESSION_TOKEN}  ${STS_SESSION_TOKEN}

Verify Limited-Scoped Token Accesses
    Configure STS Profile         ${USER_A_T1_STS_ACCESS_KEY_ID}  ${USER_A_T1_STS_SECRET_KEY}  ${USER_A_T1_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET} but not ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}
    # nor ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.  Also verify that it CANNOT write to ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Fail        ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

    # This token should NOT be able to read from/write to ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET} (because of the session policy) which is linked to
    # ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}.  It should not be able to read from/write to ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    # even though the ${TENANT_ONE_ROLE} has access because the permanent credential only has access to ${TENANT_ONE}.
    Get Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  AccessDenied
    Put Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  AccessDenied
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket

    Configure STS Profile         ${USER_A_T2_STS_ACCESS_KEY_ID}  ${USER_A_T2_STS_SECRET_KEY}  ${USER_A_T2_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET} but not ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    # nor ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET} nor ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}
    # nor ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    # Also verify that it CANNOT write to ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  AccessDenied
    Get Object Should Fail        ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Fail        ${TENANT_TWO_ICEBERG_BUCKET}   ${ICEBERG_BUCKET_TESTFILE}  AccessDenied

    Configure STS Profile         ${USER_B_T3_STS_ACCESS_KEY_ID}  ${USER_B_T3_STS_SECRET_KEY}  ${USER_B_T3_STS_SESSION_TOKEN}

    # This token should be able to read from ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET} but not ${TENANT_ONE}/${TENANT_ONE_ICEBERG_BUCKET}
    # nor ${TENANT_TWO}/${TENANT_TWO_ICEBERG_BUCKET} nor ${TENANT_TWO}/${TENANT_TWO_ANOTHER_BUCKET}
    # nor ${TENANT_ONE}/${TENANT_ONE_LINKED_BUCKET}.
    # Also verify that it CANNOT write to ${TENANT_THREE}/${TENANT_THREE_ICEBERG_BUCKET}.
    Get Object Should Succeed     ${TENANT_THREE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}
    Get Object Should Fail        ${TENANT_ONE_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ICEBERG_BUCKET}  ${ICEBERG_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_TWO_ANOTHER_BUCKET}  ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Get Object Should Fail        ${TENANT_ONE_LINKED_BUCKET}   ${ANOTHER_BUCKET_TESTFILE}  NoSuchBucket
    Put Object Should Fail        ${TENANT_THREE_ICEBERG_BUCKET}   ${ICEBERG_BUCKET_TESTFILE}  AccessDenied
