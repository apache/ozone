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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Read data from previously created key
    ${random} =         Generate Random String  5  [NUMBERS]
    ${output} =         Execute          ozone sh key get /${PREFIX}-volume/${PREFIX}-bucket/${PREFIX}-key /tmp/key-${random}
                        Should not contain  ${output}       Failed

Setup credentials for S3
# TODO: Run "Setup secure v4 headers" instead when security is enabled
    Run Keyword         Setup dummy credentials for S3

Read data from previously created key using S3 API
    ${result} =         Execute AWSS3APICli and checkrc    get-object --bucket ${PREFIX}-bucket --key key1 /tmp/key1.result    0
    # Note: "Compare files" doesn't work on NOTICE.txt as it is updated in new Ozone versions.
#                        Compare files    /opt/hadoop/NOTICE.txt    /tmp/key1.result
                        Execute and checkrc    rm /tmp/key1.result    0

Create bucket using S3 API if the bucket doesn't exist
    # Note: s3api doesn't return error if the bucket already exists
    ${result} =         Create bucket with name    ${S3PREFIX}-bucket

Write key using S3 API
    ${result} =         Execute AWSS3APICli and checkrc    put-object --bucket ${S3PREFIX}-bucket --key key2 --body /opt/hadoop/NOTICE.txt    0

Read key using S3 API
    ${result} =         Execute AWSS3APICli and checkrc    get-object --bucket ${S3PREFIX}-bucket --key key2 /tmp/key2.result    0
#                        Compare files    /opt/hadoop/NOTICE.txt    /tmp/key2.result
                        Execute and checkrc    rm /tmp/key2.result    0
