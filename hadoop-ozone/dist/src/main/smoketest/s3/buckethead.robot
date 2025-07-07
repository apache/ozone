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

*** Test Cases ***

Head Bucket
    ${result} =         Execute AWSS3APICli     head-bucket --bucket ${BUCKET}

Head Bucket not existent
    [tags]    no-bucket-type
    ${randStr} =        Generate Ozone String
    ${result} =         Execute AWSS3APICli and checkrc      head-bucket --bucket ozonenosuchbucketqqweqwe-${randStr}  255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found

Check bucket ownership verification
    ${result} =  Execute AWSS3APICli and checkrc              head-bucket --bucket ${BUCKET} --expected-bucket-owner wrong-owner  255
                 Should contain                               ${result}  403

    ${correct_owner} =    Get bucket owner                    ${BUCKET}
    Execute AWSS3APICli using bucket ownership verification   head-bucket --bucket ${BUCKET}    ${correct_owner}
