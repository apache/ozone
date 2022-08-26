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
${KEY_NAME}                    key1

*** Keywords ***
Execute AWSS3APICli
    [Arguments]       ${command}
    ${output} =       Execute                    aws s3api --endpoint-url ${ENDPOINT_URL} ${command}
    [return]          ${output}

Execute AWSS3APICli and checkrc
    [Arguments]       ${command}                 ${expected_error_code}
    ${output} =       Execute and checkrc        aws s3api --endpoint-url ${ENDPOINT_URL} ${command}  ${expected_error_code}
    [return]          ${output}

Execute AWSS3Cli
    [Arguments]       ${command}
    ${output} =       Execute                     aws s3 --endpoint-url ${ENDPOINT_URL} ${command}
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
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user    testuser    testuser.keytab
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Setup secure v4 headers
    Run Keyword if      '${SECURITY_ENABLED}' == 'false'    Setup dummy credentials for S3

Setup secure v4 headers
    ${result} =         Execute                    ozone s3 getsecret ${OM_HA_PARAM}
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

Setup dummy credentials for S3
                        Execute                    aws configure set default.s3.signature_version s3v4
                        Execute                    aws configure set aws_access_key_id dlfknslnfslf
                        Execute                    aws configure set aws_secret_access_key dlfknslnfslf
                        Execute                    aws configure set region us-west-1

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
    Run Keyword        Generate random prefix
    Run Keyword        Install aws cli
    Run Keyword if    '${OZONE_S3_SET_CREDENTIALS}' == 'true'    Setup v4 headers
    ${BUCKET} =        Run Keyword if                            '${BUCKET}' == 'generated'            Create bucket
    ...                ELSE                                      Set Variable    ${BUCKET}
                       Set Suite Variable                        ${BUCKET}
                       Run Keyword if                            '${BUCKET}' == 'link'                 Setup links for S3 tests
                       Run Keyword if                            '${BUCKET}' == 'encrypted'            Create encrypted bucket
                       Run Keyword if                            '${BUCKET}' == 'erasure'              Create EC bucket

Setup links for S3 tests
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/link
    Return From Keyword If    ${exists}
    Execute            ozone sh volume create o3://${OM_SERVICE_ID}/legacy
    Execute            ozone sh bucket create o3://${OM_SERVICE_ID}/legacy/source-bucket
    Create link        link

Create encrypted bucket
    Return From Keyword if    '${SECURITY_ENABLED}' == 'false'
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/encrypted
    Return From Keyword If    ${exists}
    Execute            ozone sh bucket create -k ${KEY_NAME} o3://${OM_SERVICE_ID}/s3v/encrypted

Create link
    [arguments]       ${bucket}
    Execute           ozone sh bucket link o3://${OM_SERVICE_ID}/legacy/source-bucket o3://${OM_SERVICE_ID}/s3v/${bucket}
    [return]          ${bucket}

Create EC bucket
    ${exists} =        Bucket Exists    o3://${OM_SERVICE_ID}/s3v/erasure
    Return From Keyword If    ${exists}
    Execute            ozone sh bucket create --replication rs-3-2-1024k --type EC o3://${OM_SERVICE_ID}/s3v/erasure

Generate random prefix
    ${random} =          Generate Ozone String
                         Set Suite Variable  ${PREFIX}  ${random}

Perform Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${bucket} --key ${key}
    ${upload_id} =      Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0

    @{etags} =    Create List
    FOR    ${i}    ${file}    IN ENUMERATE    @{files}
        ${part} =    Evaluate    ${i} + 1
        ${result} =   Execute AWSS3APICli     upload-part --bucket ${bucket} --key ${key} --part-number ${part} --body ${file} --upload-id ${upload_id}
        ${etag} =     Execute                 echo '${result}' | jq -r '.ETag'
        Append To List    ${etags}    {ETag=${etag},PartNumber=${part}}
    END

    ${parts} =    Catenate    SEPARATOR=,    @{etags}
    Execute AWSS3APICli     complete-multipart-upload --bucket ${bucket} --key ${key} --upload-id ${upload_id} --multipart-upload 'Parts=[${parts}]'

Verify Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${random} =    Generate Ozone String

    Execute AWSS3APICli     get-object --bucket ${bucket} --key ${key} /tmp/verify${random}
    ${tmp} =    Catenate    @{files}
    Execute    cat ${tmp} > /tmp/original${random}
    Compare files    /tmp/original${random}    /tmp/verify${random}
