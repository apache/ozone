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
Copy Object Happy Scenario
    [Arguments]         ${bucket}    ${destbucket}

    ${postfix} =        Generate Random String  5            [NUMBERS]
    ${key} =            Put new object          ${bucket}    copyobject/put${postfix}
    ${prefix} =         Set Variable            copyobject/copy

    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${destbucket} --key ${prefix}${postfix} --copy-source ${bucket}/${key}
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${destbucket} --prefix ${prefix}
                        Should contain             ${result}         ${prefix}${postfix}
    #copying again will not throw error
                        Execute AWSS3ApiCli        copy-object --bucket ${destbucket} --key ${prefix}${postfix} --copy-source ${bucket}/${key}

Copy Object Where Bucket is not available
    [Arguments]         ${bucket}

    ${key} =            Put new object    ${bucket}    copyobject/put
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket dfdfdfdfdfnonexistent --key any --copy-source ${bucket}/${key}    255
                        Should contain             ${result}        NoSuchBucket
    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${bucket} --key any --copy-source dfdfdfdfdfnonexistent/${key}    255
                        Should contain             ${result}        NoSuchBucket

Copy Object Where both source and dest are same with change to storageclass
    [Arguments]         ${bucket}

    ${key} =            Put new object    ${bucket}    copyobject/put
    ${result} =         Execute AWSS3APICli        copy-object --storage-class REDUCED_REDUNDANCY --bucket ${bucket} --key ${key} --copy-source ${bucket}/${key}
                        Should contain             ${result}        ETag

Copy Object Where Key not available
    [Arguments]         ${bucket}

    ${result} =         Execute AWSS3APICli and checkrc        copy-object --bucket ${bucket} --key any --copy-source ${bucket}/nonnonexistentkey       255
                        Should contain             ${result}        NoSuchKey

*** Test Cases ***

Copy Object Happy Scenario
    [Template]    Copy Object Happy Scenario
    ${BUCKET}        ${BUCKET}
    ${BUCKET}        ${BUCKET_LINK}
    ${BUCKET_LINK}   ${BUCKET}
    ${BUCKET_LINK}   ${BUCKET_LINK}

Copy Object Where Bucket is not available
    [Template]    Copy Object Where Bucket is not available
    ${BUCKET}
    ${BUCKET_LINK}

Copy Object Where both source and dest are same with change to storageclass
    [Template]    Copy Object Where both source and dest are same with change to storageclass
    ${BUCKET}
    ${BUCKET_LINK}

Copy Object Where Key not available
    [Template]    Copy Object Where Key not available
    ${BUCKET}
    ${BUCKET_LINK}

