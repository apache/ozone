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
Library             OperatingSystem
Library             String
Library             BuiltIn
Library             DateTime
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot

*** Variables ***
${RANGER_ENDPOINT_URL}            ${EMPTY}
${STS_ENDPOINT_URL}               http://s3g:9880/sts
${S3G_ENDPOINT_URL}               http://s3g:9878
${ROLE_SESSION_NAME}              sts-session-name

*** Keywords ***
Configure AWS Profile
    [Arguments]                   ${profile}  ${access_key}  ${secret_key}  ${session_token}=${EMPTY}  ${region}=us-east-1
    Run Keyword                   Install aws cli
    # Use v4 signatures so presign URL will work (Ozone rejects v2 signatures)
    Execute                       aws configure set s3.signature_version s3v4 --profile ${profile}
    Execute                       aws configure set aws_access_key_id ${access_key} --profile ${profile}
    Execute                       aws configure set aws_secret_access_key ${secret_key} --profile ${profile}
    Execute                       aws configure set region ${region} --profile ${profile}
    Run Keyword If                '${session_token}' != '${EMPTY}'    Execute  aws configure set aws_session_token ${session_token} --profile ${profile}

Configure STS Profile
    [Arguments]                   ${access_key}  ${secret_key}  ${session_token}
    Configure AWS Profile         sts  ${access_key}  ${secret_key}  ${session_token}

Create Ranger Artifact
    [Arguments]                   ${json}                       ${endpoint_url}
    Pass Execution If             '${RANGER_ENDPOINT_URL}' == ''  No Ranger
    ${result} =                   Execute                       curl --fail --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '${json}' '${endpoint_url}'
    Should Contain                ${result}                     HTTP/1.1 200

Create Ranger User
    [Arguments]                   ${user_json}
    # Note: the /service/xusers/secure/users endpoint must be used below so that the userPermList can be set.  Without
    # the userPermList being set, the user cannot be added to a Ranger policy.
    Create Ranger Artifact        ${user_json}                  '${RANGER_ENDPOINT_URL}/service/xusers/secure/users'

Create Ranger Role
    [Arguments]                   ${role_json}
    Create Ranger Artifact        ${role_json}                  '${RANGER_ENDPOINT_URL}/service/roles/roles'

Create Ranger Policy
    [Arguments]                   ${policy_json}
    Create Ranger Artifact        ${policy_json}                '${RANGER_ENDPOINT_URL}/service/public/v2/api/policy'

Assume Role And Get Temporary Credentials
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${role_arn}=${ROLE_ARN}
    Configure AWS Profile         permanent  ${perm_access_key_id}  ${perm_secret_key}

    ${now} =                      Get Current Date              time_zone=UTC

    ${cmd} =                      Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${ROLE_ARN} --role-session-name ${ROLE_SESSION_NAME} --duration-seconds 900 --output json --profile permanent
    ${cmd} =                      Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

    ${json} =                     Execute                       ${cmd}
    Should contain                ${json}                       Credentials

    ${stsAccessKeyId} =           Execute                       echo '${json}' | jq -r '.Credentials.AccessKeyId'
    ${stsSecretKey} =             Execute                       echo '${json}' | jq -r '.Credentials.SecretAccessKey'
    ${stsSessionToken} =          Execute                       echo '${json}' | jq -r '.Credentials.SessionToken'
    Should Start With             ${stsAccessKeyId}             ASIA
    Set Global Variable           ${STS_ACCESS_KEY_ID}          ${stsAccessKeyId}
    Set Global Variable           ${STS_SECRET_KEY}             ${stsSecretKey}
    Set Global Variable           ${STS_SESSION_TOKEN}          ${stsSessionToken}

    # Verify Expiration is at least 900 seconds in the future, but allow 2 second grace on both sides for clock skew
    ${expiration} =               Execute                       echo '${json}' | jq -r '.Credentials.Expiration'
    ${time_diff} =                Subtract Date From Date       ${expiration}    ${now}
    Should Be True                ${time_diff} >= 898           Expected expiration to be at least 898s in the future, but was ${time_diff}s
    Should Be True                ${time_diff} <= 902           Expected expiration to be at most 902s in the future, but was ${time_diff}s

Assume Role Should Fail
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${expected_error}=AccessDenied
    Configure AWS Profile         permanent  ${perm_access_key_id}  ${perm_secret_key}

    ${cmd} =                      Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${ROLE_ARN} --role-session-name ${ROLE_SESSION_NAME} --duration-seconds 900 --output json --profile permanent
    ${cmd} =                      Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

    ${output} =                   Execute And Ignore Error      ${cmd}
    Should Contain                ${output}                     ${expected_error}

Get Object Should Succeed
    [Arguments]                   ${bucket}  ${key}  ${destination}=./${key}  ${profile}=sts
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object --bucket ${bucket} --key ${key} ${destination} --profile ${profile}
    Should Contain                ${output}                     "AcceptRanges": "bytes"

Get Object Should Fail
    [Arguments]                   ${bucket}  ${key}  ${expectedFailureMessage}  ${destination}=./${key}  ${profile}=sts
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} get-object --bucket ${bucket} --key ${key} ${destination} --profile ${profile}
    Should Contain                ${output}                     ${expectedFailureMessage}

Put Object Should Succeed
    [Arguments]                   ${bucket}  ${key}  ${body}=./${key}  ${profile}=sts
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object --bucket ${bucket} --key ${key} --body ${body} --profile ${profile}
    Should Contain                ${output}                     "ETag"

Put Object Should Fail
    [Arguments]                   ${bucket}  ${key}  ${expectedFailureMessage}  ${body}=./${key}  ${profile}=sts
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} put-object --bucket ${bucket} --key ${key} --body ${body} --profile ${profile}
    Should Contain                ${output}                     ${expectedFailureMessage}

Create Bucket Should Fail
    [Arguments]                   ${bucket}  ${expectedFailureMessage}=AccessDenied  ${profile}=sts
    ${output} =                   Execute And Ignore Error      aws s3api --endpoint-url ${S3G_ENDPOINT_URL} create-bucket --bucket ${bucket} --profile ${profile}
    Should Contain                ${output}                     ${expectedFailureMessage}
