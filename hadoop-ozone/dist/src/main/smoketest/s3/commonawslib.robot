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
Library             Collections
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot

*** Variables ***
${ENDPOINT_URL}                http://s3g:9878
${OZONE_S3_HEADER_VERSION}     v4
${OZONE_S3_SET_CREDENTIALS}    true
${BUCKET}                      generated
${BUCKET_LAYOUT}               OBJECT_STORE
${ENCRYPTION_KEY}              key1
${OZONE_S3_TESTS_SET_UP}       ${FALSE}
${OZONE_AWS_ACCESS_KEY_ID}     ${EMPTY}
${OZONE_S3_ADDRESS_STYLE}      path

*** Keywords ***
Execute AWSS3APICli
    [Arguments]       ${command}
    ${output} =       Execute                    aws s3api --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

# For possible AWS CLI return codes see: https://docs.aws.amazon.com/cli/latest/topic/return-codes.html
Execute AWSS3APICli and checkrc
    [Arguments]       ${command}                 ${expected_error_code}
    ${output} =       Execute and checkrc        aws s3api --endpoint-url ${ENDPOINT_URL} ${command}  ${expected_error_code}
    [return]          ${output}

Execute AWSS3APICli and ignore error
    [Arguments]       ${command}
    ${output} =       Execute And Ignore Error   aws s3api --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

Execute AWSS3Cli
    [Arguments]       ${command}
    ${output} =       Execute                     aws s3 --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

Execute AWSS3CliDebug
    [Arguments]       ${command}
    ${output} =       Execute                     aws --debug s3 --endpoint ${ENDPOINT_URL} ${command}
    [return]          ${output}

Install aws cli
    ${rc}              ${output} =                 Run And Return Rc And Output           which aws
    Return From Keyword If    '${rc}' == '0'
    ${rc}              ${output} =                 Run And Return Rc And Output           which apt-get
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 debian
    ${rc}              ${output} =                 Run And Return Rc And Output           yum --help
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 centos

Install aws cli s3 centos
    Execute            sudo -E yum install -y awscli

Install aws cli s3 debian
    Execute            sudo -E apt-get install -y awscli

Setup v2 headers
                        Set Environment Variable   AWS_ACCESS_KEY_ID       ANYID
                        Set Environment Variable   AWS_SECRET_ACCESS_KEY   ANYKEY

Setup v4 headers
    Get Security Enabled From Config
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user    testuser    testuser.keytab
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Setup secure v4 headers
    Run Keyword if      '${SECURITY_ENABLED}' == 'false'    Setup dummy credentials for S3

Setup secure v4 headers
    ${result} =         Execute and Ignore error             ozone s3 getsecret ${OM_HA_PARAM}
    ${exists} =         Run Keyword And Return Status    Should Contain    ${result}    S3_SECRET_ALREADY_EXISTS
    IF                  ${exists}
                        Execute    ozone s3 revokesecret -y ${OM_HA_PARAM}
        ${result} =     Execute    ozone s3 getsecret ${OM_HA_PARAM}
    END

    ${accessKey} =      Get Regexp Matches         ${result}     (?<=awsAccessKey=).*
    # Use a valid user that are created in the Docket image Ex: testuser if it is not a secure cluster
    ${accessKey} =      Get Variable Value         ${accessKey}  testuser
    ${secret} =         Get Regexp Matches         ${result}     (?<=awsSecret=).*
    ${accessKey} =      Set Variable               ${accessKey[0]}
    ${secret} =         Set Variable               ${secret[0]}
                        Execute                    aws configure set default.s3.signature_version s3v4
                        Execute                    aws configure set aws_access_key_id ${accessKey}
                        Execute                    aws configure set aws_secret_access_key ${secret}
                        Execute                    aws configure set region us-west-1
                        Execute                    aws configure set default.s3.addressing_style ${OZONE_S3_ADDRESS_STYLE}


Setup dummy credentials for S3
                        Execute                    aws configure set default.s3.signature_version s3v4
                        Execute                    aws configure set aws_access_key_id dlfknslnfslf
                        Execute                    aws configure set aws_secret_access_key dlfknslnfslf
                        Execute                    aws configure set region us-west-1

Save AWS access key
    ${OZONE_AWS_ACCESS_KEY_ID} =      Execute     aws configure get aws_access_key_id
    Set Test Variable     ${OZONE_AWS_ACCESS_KEY_ID}

Restore AWS access key
    Execute    aws configure set aws_access_key_id ${OZONE_AWS_ACCESS_KEY_ID}

Generate Ozone String
    ${randStr} =         Generate Random String     10  [NUMBERS]
    [Return]             ozone-test-${randStr}

Create bucket
    ${postfix} =         Generate Ozone String
    ${bucket} =          Set Variable               bucket-${postfix}
                         Create bucket with name    ${bucket}
    [Return]             ${bucket}

Create bucket with name
    [Arguments]          ${bucket}
    ${result} =          Execute AWSS3APICli  create-bucket --bucket ${bucket}
                         Should contain              ${result}         Location
                         Should contain              ${result}         ${bucket}

Setup s3 tests
    Return From Keyword if    ${OZONE_S3_TESTS_SET_UP}
    Run Keyword        Generate random prefix
    Run Keyword        Install aws cli
    Run Keyword        Get Security Enabled From Config
    Run Keyword if    '${OZONE_S3_SET_CREDENTIALS}' == 'true'    Setup v4 headers
    Run Keyword if    '${BUCKET}' == 'generated'            Create generated bucket    ${BUCKET_LAYOUT}
    Run Keyword if    '${BUCKET}' == 'link'                 Setup links for S3 tests
    Run Keyword if    '${BUCKET}' == 'encrypted'            Create encrypted bucket
    Run Keyword if    '${BUCKET}' == 'erasure'              Create EC bucket
    Set Global Variable  ${OZONE_S3_TESTS_SET_UP}    ${TRUE}

Setup links for S3 tests
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/link
    Return From Keyword If    ${exists}
    Execute            ozone sh volume create o3://${OM_SERVICE_ID}/legacy
    Execute            ozone sh bucket create --layout ${BUCKET_LAYOUT} o3://${OM_SERVICE_ID}/legacy/source-bucket
    Create link        link

Create generated bucket
    [Arguments]          ${layout}=OBJECT_STORE
    ${BUCKET} =          Create bucket with layout    s3v    ${layout}
    Set Global Variable   ${BUCKET}

Create encrypted bucket
    Return From Keyword if    '${SECURITY_ENABLED}' == 'false'
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/encrypted
    Return From Keyword If    ${exists}
    Execute            ozone sh bucket create -k ${ENCRYPTION_KEY} --layout ${BUCKET_LAYOUT} o3://${OM_SERVICE_ID}/s3v/encrypted

Create link
    [arguments]       ${bucket}
    Execute           ozone sh bucket link o3://${OM_SERVICE_ID}/legacy/source-bucket o3://${OM_SERVICE_ID}/s3v/${bucket}
    [return]          ${bucket}

Create EC bucket
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/erasure
    Return From Keyword If    ${exists}
    Execute            ozone sh bucket create --replication rs-3-2-1024k --type EC --layout ${BUCKET_LAYOUT} o3://${OM_SERVICE_ID}/s3v/erasure

Generate random prefix
    ${random} =          Generate Ozone String
                         Set Global Variable  ${PREFIX}  ${random}

# Verify object put by listing and getting it
Put object to bucket
    [arguments]    ${bucket}    ${key}    ${path}

    Execute AWSS3ApiCli    put-object --bucket ${bucket} --key ${key} --body ${path}

    ${result} =    Execute AWSS3ApiCli    list-objects --bucket ${bucket}
    Should contain    ${result}    ${key}

    Execute AWSS3ApiCli    get-object --bucket ${bucket} --key ${key} ${path}.verify
    Compare files          ${path}    ${path}.verify

    [teardown]    Remove File    ${path}.verify

Revoke S3 secrets
    Execute and Ignore Error             ozone s3 revokesecret -y
    Execute and Ignore Error             ozone s3 revokesecret -y -u testuser
    Execute and Ignore Error             ozone s3 revokesecret -y -u testuser2

Get bucket owner
    [arguments]    ${bucket}
    ${owner} =     Execute    aws s3api --endpoint-url ${ENDPOINT_URL} get-bucket-acl --bucket ${bucket} | jq -r .Owner.DisplayName
    [return]       ${owner}

Execute AWSS3APICli using bucket ownership verification
    [arguments]    ${command}    ${expected_bucket_owner}    ${expected_source_bucket_owner}=${EMPTY}
    ${cmd} =       Set Variable           ${command} --expected-bucket-owner ${expected_bucket_owner}
    ${cmd} =       Set Variable If        '${expected_source_bucket_owner}' != '${EMPTY}'    ${cmd} --expected-source-bucket-owner ${expected_source_bucket_owner}    ${cmd}
    ${result} =    Execute AWSS3APICli    ${cmd}
    Should Not Contain    ${result}    Access Denied
    [return]              ${result}

Execute AWSS3APICli and failed bucket ownership verification
    [arguments]    ${command}    ${wrong_bucket_owner}    ${wrong_source_bucket_owner}=${EMPTY}
    ${cmd} =       Set Variable           ${command} --expected-bucket-owner ${wrong_bucket_owner}
    ${cmd} =       Set Variable If        '${wrong_source_bucket_owner}' != '${EMPTY}'    ${cmd} --expected-source-bucket-owner ${wrong_source_bucket_owner}    ${cmd}
    ${result} =    Execute AWSS3APICli and ignore error    ${cmd}
    Should contain      ${result}         Access Denied

Execute AWSS3APICli with bucket owner check
    [arguments]    ${command}    ${bucket_owner}    ${source_bucket_owner}=${EMPTY}

    Run Keyword If    '${source_bucket_owner}' != '${EMPTY}'    Execute AWSS3APICli and failed bucket ownership verification    ${command}    wrong-${bucket_owner}    ${source_bucket_owner}
    Run Keyword If    '${source_bucket_owner}' != '${EMPTY}'    Execute AWSS3APICli and failed bucket ownership verification    ${command}    ${bucket_owner}    wrong-${source_bucket_owner}
    Run Keyword If    '${source_bucket_owner}' == '${EMPTY}'    Execute AWSS3APICli and failed bucket ownership verification    ${command}    wrong-${bucket_owner}
    ${result} =    Execute AWSS3APICli using bucket ownership verification    ${command}    ${bucket_owner}    ${source_bucket_owner}
    [return]              ${result}
