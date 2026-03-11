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
Library             Collections
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
    ${result} =                   Execute                       curl --silent --show-error --include --location --netrc --request POST --header "Content-Type: application/json" --header "accept: application/json" --data '${json}' '${endpoint_url}'
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

Create Ranger Assume Role Policy
    [Arguments]                   ${role_name}  ${user_name}
    ${policy_json} =              Set Variable                  { "isEnabled": true, "service": "dev_ozone", "name": "${role_name} assume role policy", "policyType": 0, "policyPriority": 0, "isAuditEnabled": true, "resources": { "role": { "values": ["${role_name}"], "isExcludes": false, "isRecursive": false } }, "policyItems": [ { "accesses": [ { "type": "assume_role", "isAllowed": true } ], "users": [ "${user_name}" ], "delegateAdmin": false } ], "serviceType": "ozone", "isDenyAllElse": false }
    Create Ranger Policy          ${policy_json}

Update Ranger Policy Items
    [Arguments]                   ${policy_name}  ${policy_item_json}
    # Fetch the existing policy json, update by id.
    ${encoded_name} =             Execute                       python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "${policy_name}"
    ${policy} =                   Execute                       curl --silent --show-error --location --netrc --request GET --header "accept: application/json" "${RANGER_ENDPOINT_URL}/service/public/v2/api/service/dev_ozone/policy/${encoded_name}"
    ${policy_id} =                Execute                       printf '%s' '${policy}' | jq -r '.id // empty'
    Should Not Be Empty           ${policy_id}
    ${updated} =                  Execute                       printf '%s' '${policy}' | jq '.policyItems += ${policy_item_json}'
    ${result} =                   Execute                       curl --silent --show-error --include --location --netrc -X PUT -H "Content-Type: application/json" -H "accept: application/json" --data '${updated}' "${RANGER_ENDPOINT_URL}/service/public/v2/api/policy/${policy_id}"
    Should Contain                ${result}                     HTTP/1.1 200

Assume Role And Get Temporary Credentials
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${role_arn}=${ROLE_ARN_OBS}  ${role_session_name}=${ROLE_SESSION_NAME}  ${duration_seconds}=900
    Configure AWS Profile         permanent  ${perm_access_key_id}  ${perm_secret_key}

    ${cmd} =                      Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${role_arn} --role-session-name ${role_session_name} --output json --profile permanent
    ${cmd} =                      Set Variable If               '${duration_seconds}' != '${EMPTY}'    ${cmd} --duration-seconds ${duration_seconds}    ${cmd}
    ${cmd} =                      Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

    ${json} =                     Execute                       ${cmd}

    # Don't include the latency of the AssumeRole call when checking the expiration
    ${now} =                      Get Current Date              time_zone=UTC

    Should Contain                ${json}                       Credentials

    ${stsAccessKeyId} =           Execute                       echo '${json}' | jq -r '.Credentials.AccessKeyId'
    ${stsSecretKey} =             Execute                       echo '${json}' | jq -r '.Credentials.SecretAccessKey'
    ${stsSessionToken} =          Execute                       echo '${json}' | jq -r '.Credentials.SessionToken'
    Should Start With             ${stsAccessKeyId}             ASIA
    Set Global Variable           ${STS_ACCESS_KEY_ID}          ${stsAccessKeyId}
    Set Global Variable           ${STS_SECRET_KEY}             ${stsSecretKey}
    Set Global Variable           ${STS_SESSION_TOKEN}          ${stsSessionToken}

    ${expected_duration} =        Set Variable                  ${duration_seconds}
    # Ensure the expected duration defaults to 3600 seconds (1 hour) if not specified
    ${expected_duration} =        Set Variable If               '${duration_seconds}' == '${EMPTY}'    3600    ${expected_duration}
    ${minimum_expected} =         Evaluate                      int(${expected_duration}) - 2
    ${maximum_expected} =         Evaluate                      int(${expected_duration}) + 2

    # Verify Expiration based on requested duration (or default 1h), with small grace (plus or minus 2 seconds) for clock skew.
    ${expiration} =               Execute                       echo '${json}' | jq -r '.Credentials.Expiration'
    ${time_diff} =                Subtract Date From Date       ${expiration}    ${now}
    Should Be True                ${time_diff} >= ${minimum_expected}           Expected expiration to be at least ${minimum_expected}s in the future, but was ${time_diff}s
    Should Be True                ${time_diff} <= ${maximum_expected}           Expected expiration to be at most ${maximum_expected}s in the future, but was ${time_diff}s

Assume Role And Configure STS Profile
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${role_arn}=${ROLE_ARN_OBS}  ${role_session_name}=${ROLE_SESSION_NAME}  ${duration_seconds}=900
    Assume Role And Get Temporary Credentials                   perm_access_key_id=${perm_access_key_id}  perm_secret_key=${perm_secret_key}  policy_json=${policy_json}  role_arn=${role_arn}  role_session_name=${role_session_name}  duration_seconds=${duration_seconds}
    Configure STS Profile         ${STS_ACCESS_KEY_ID}  ${STS_SECRET_KEY}  ${STS_SESSION_TOKEN}

Assume Role Should Fail
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${expected_error}=AccessDenied  ${expected_http_code}=${EMPTY}  ${role_arn}=${ROLE_ARN_OBS}  ${role_session_name}=${ROLE_SESSION_NAME}  ${duration_seconds}=900
    Configure AWS Profile         permanent  ${perm_access_key_id}  ${perm_secret_key}

    IF                            '${expected_http_code}' != '${EMPTY}'
        # Note: curl in the s3g container doesn't reliably support --aws-sigv4,
        # so use awscli debug output to capture the HTTP response code.
        ${cmd} =                  Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${role_arn} --role-session-name ${role_session_name} --profile permanent --debug 2>&1
        ${cmd} =                  Set Variable If               '${duration_seconds}' != '${EMPTY}'    ${cmd} --duration-seconds ${duration_seconds}    ${cmd}
        ${cmd} =                  Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

        ${output} =               Execute And Ignore Error      ${cmd}
        Should Contain            ${output}                     ${expected_error}

        @{http_codes} =           Get Regexp Matches            ${output}    (?m)^.*"POST .*" ([0-9]{3}) .*    1
        ${code_count} =           Get Length                    ${http_codes}
        Should Be True            ${code_count} > 0             Expected to find an HTTP status code in awscli --debug output, but none was found.
        ${http_code} =            Get From List                 ${http_codes}    -1
        Should Be Equal As Strings  ${http_code}                ${expected_http_code}
    ELSE
        ${cmd} =                  Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${role_arn} --role-session-name ${role_session_name} --output json --profile permanent
        ${cmd} =                  Set Variable If               '${duration_seconds}' != '${EMPTY}'    ${cmd} --duration-seconds ${duration_seconds}    ${cmd}
        ${cmd} =                  Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}

        ${output} =               Execute And Ignore Error      ${cmd}
        Should Contain            ${output}                     ${expected_error}
    END

Assume Role Should Fail Using Curl
    # This keyword is needed to test boundary cases that the aws client prevents you from issuing, such as too short duration for token
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${expected_error}=AccessDenied  ${expected_http_code}=400  ${role_arn}=${ROLE_ARN_OBS}  ${role_session_name}=${ROLE_SESSION_NAME}  ${duration_seconds}=900
    ${cmd} =                      Set Variable                  curl --silent --show-error --include --request POST --aws-sigv4 "aws:amz:us-east-1:sts" --user '${perm_access_key_id}:${perm_secret_key}' --header "Content-Type: application/x-www-form-urlencoded" --data-urlencode "Action=AssumeRole" --data-urlencode "Version=2011-06-15" --data-urlencode "RoleArn=${role_arn}" --data-urlencode "RoleSessionName=${role_session_name}" ${STS_ENDPOINT_URL}
    ${cmd} =                      Set Variable If               '${duration_seconds}' != '${EMPTY}'    ${cmd} --data-urlencode "DurationSeconds=${duration_seconds}"    ${cmd}
    ${cmd} =                      Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --data-urlencode "Policy=${policy_json}"    ${cmd}
    ${output} =                   Execute And Ignore Error      ${cmd}
    Should Contain                ${output}                     ${expected_error}
    @{http_codes} =               Get Regexp Matches            ${output}    (?m)^HTTP/[0-9.]+ ([0-9]{3})    1
    ${code_count} =               Get Length                    ${http_codes}
    Should Be True                ${code_count} > 0             Expected to find an HTTP status code in curl output, but none was found.
    ${http_code} =                Get From List                 ${http_codes}    -1
    Should Be Equal As Strings    ${http_code}                  ${expected_http_code}

Get Assume Role Debug Output
    # This keyword is needed to check headers on the AssumeRole to ensure they comply with AWS
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${policy_json}=${EMPTY}  ${role_arn}=${ROLE_ARN_OBS}  ${role_session_name}=${ROLE_SESSION_NAME}  ${duration_seconds}=900
    Configure AWS Profile         permanent  ${perm_access_key_id}  ${perm_secret_key}
    ${cmd} =                      Set Variable                  aws sts assume-role --endpoint-url ${STS_ENDPOINT_URL} --role-arn ${role_arn} --role-session-name ${role_session_name} --profile permanent --debug 2>&1
    ${cmd} =                      Set Variable If               '${duration_seconds}' != '${EMPTY}'    ${cmd} --duration-seconds ${duration_seconds}    ${cmd}
    ${cmd} =                      Set Variable If               '${policy_json}' != '${EMPTY}'    ${cmd} --policy '${policy_json}'    ${cmd}
    ${output} =                   Execute And Ignore Error      ${cmd}
    [Return]                      ${output}

Assume Role Response Headers Should Be Present
    [Arguments]                   ${perm_access_key_id}  ${perm_secret_key}  ${role_arn}=${ROLE_ARN_OBS}
    ${output} =                   Get Assume Role Debug Output  ${perm_access_key_id}  ${perm_secret_key}  role_arn=${role_arn}
    Should Contain                ${output}                     X-Amz-Sts-Extended-Request-Id
    Should Contain                ${output}                     x-amzn-RequestId

Generate Oversized Session Policy
    ${policy} =                   Execute                       python3 /opt/hadoop/smoketest/security/generate_oversized_session_policy.py
    [Return]                      ${policy}

Mutate STS Session Token
    [Arguments]                   ${session_token}  ${mutation_type}
    ${mutated} =                  Execute                       SESSION_TOKEN='${session_token}' MUTATION_TYPE='${mutation_type}' python3 /opt/hadoop/smoketest/security/mutate_sts_session_token.py
    [Return]                      ${mutated}

Assign User To Tenant And Get Credentials
    [Arguments]                   ${user}  ${tenant}
    ${output} =                   Execute                       ozone tenant --verbose user assign ${user} --tenant=${tenant}
    Should Contain                ${output}                     Assigned '${user}' to '${tenant}'
    @{access_key_matches} =       Get Regexp Matches            ${output}    (?m)(?<=export AWS_ACCESS_KEY_ID=).*$
    @{secret_key_matches} =       Get Regexp Matches            ${output}    (?m)(?<=export AWS_SECRET_ACCESS_KEY=).*$
    ${access_key_id} =            Strip String                  ${access_key_matches[0]}
    ${secret_key} =               Strip String                  ${secret_key_matches[0]}
    [Return]                      ${access_key_id}              ${secret_key}

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

List Object Keys Should Succeed
    [Arguments]                   ${bucket}  ${api}=list-objects  ${request_prefix}=${EMPTY}  ${profile}=sts  ${delimiter}=${EMPTY}
    ${cmd} =                      Set Variable                  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} ${api} --bucket ${bucket} --output json --profile ${profile}
    ${cmd} =                      Set Variable If               '${request_prefix}' != '${EMPTY}'    ${cmd} --prefix ${request_prefix}    ${cmd}
    ${cmd} =                      Set Variable If               '${delimiter}' != '${EMPTY}'    ${cmd} --delimiter '${delimiter}'    ${cmd}
    ${output} =                   Execute And Ignore Error      ${cmd}
    Should Not Contain            ${output}                     AccessDenied
    ${keys_json} =                Execute                       echo '${output}' | jq -c '[(.Contents // [])[] | .Key] | sort'
    [Return]                      ${keys_json}

List Object Keys Should Fail
    [Arguments]                   ${bucket}  ${api}=list-objects  ${expected_failure}=AccessDenied  ${request_prefix}=${EMPTY}  ${profile}=sts  ${delimiter}=${EMPTY}
    ${cmd} =                      Set Variable                  aws s3api --endpoint-url ${S3G_ENDPOINT_URL} ${api} --bucket ${bucket} --output json --profile ${profile}
    ${cmd} =                      Set Variable If               '${request_prefix}' != '${EMPTY}'    ${cmd} --prefix ${request_prefix}    ${cmd}
    ${cmd} =                      Set Variable If               '${delimiter}' != '${EMPTY}'    ${cmd} --delimiter '${delimiter}'    ${cmd}
    ${output} =                   Execute And Ignore Error      ${cmd}
    Should Contain                ${output}                     ${expected_failure}

Assert Listed Keys Json Should Equal
    [Arguments]                   ${actual_keys_json}  ${expected_keys_json}
    # This compares JSON to another JSON
    ${actual_list} =              Evaluate                      json.loads($actual_keys_json)    modules=json
    ${expected_list} =            Evaluate                      json.loads($expected_keys_json)  modules=json
    Lists Should Be Equal         ${actual_list}                ${expected_list}

Assert Listed Keys Should Equal
    [Arguments]                   ${actual_keys_json}  @{expected_keys}
    # This compares JSON to a list of strings
    ${expected_sorted} =          Copy List                     ${expected_keys}
    Sort List                     ${expected_sorted}
    ${actual_list} =              Evaluate                      json.loads($actual_keys_json)    modules=json
    Lists Should Be Equal         ${actual_list}                ${expected_sorted}
