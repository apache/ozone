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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Keywords ***
Delete file with s3api
    [Arguments]         ${bucket}

    ${prefix} =         Set Variable               deletes3api/key
    ${key} =            Put new object             ${bucket}    ${prefix}
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key ${key}
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${bucket} --prefix ${prefix}
                        Should not contain         ${result}         ${key}
#In case of HTTP 500, the error code is printed out to the console.
                        Should not contain         ${result}         500

Delete file with s3api, file doesn't exist
    [Arguments]         ${bucket}

    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/
                        Should not contain         ${result}         thereisnosuchfile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key thereisnosuchfile
    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/
                        Should not contain         ${result}         thereisnosuchfile

Delete dir with s3api
    [Arguments]         ${bucket}

    ${dir} =            Set Variable               deletes3apidir
    ${key} =            Put new object             ${bucket}    ${dir}/key
    ${filename} =       Remove String              ${key}       ${dir}/
    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/${dir}/
                        Should contain             ${result}         ${filename}
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key ${dir}/
    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/${dir}/
                        Should contain             ${result}         ${filename}
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key ${key}


Delete file with s3api, file doesn't exist, prefix of a real file
    [Arguments]         ${bucket}

    ${dir} =            Set Variable               deletes3apiprefix
    ${prefix} =         Set Variable               ${dir}/file
    ${key} =            Put new object             ${bucket}    ${prefix}
    ${filename} =       Remove String              ${key}       ${dir}/
    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/${dir}/
                        Should contain             ${result}         ${filename}
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key ${prefix}
    ${result} =         Execute AWSS3Cli           ls s3://${bucket}/${dir}/
                        Should contain             ${result}         ${filename}
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${bucket} --key ${key}


*** Test Cases ***

Delete file with s3api
    [Template]    Delete file with s3api
    ${BUCKET}
    ${BUCKET_LINK}

Delete file with s3api, file doesn't exist
    [Template]    Delete file with s3api, file doesn't exist
    ${BUCKET}
    ${BUCKET_LINK}

Delete dir with s3api
    [Template]    Delete dir with s3api
    ${BUCKET}
    ${BUCKET_LINK}

Delete file with s3api, file doesn't exist, prefix of a real file
    [Template]    Delete file with s3api, file doesn't exist, prefix of a real file
    ${BUCKET}
    ${BUCKET_LINK}

Delete file with s3api, bucket doesn't exist
    ${result} =         Execute AWSS3APICli and checkrc   delete-object --bucket deletes3api-nosuchbucket --key any      255
                        Should contain                    ${result}         NoSuchBucket

