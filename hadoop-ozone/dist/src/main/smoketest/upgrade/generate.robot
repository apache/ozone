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
Documentation       Generate data
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Create a volume and bucket
    [Tags]    create-volume-and-bucket
    ${output} =         Execute          ozone sh volume create ${PREFIX}-volume
                        Should not contain  ${output}       Failed
    ${output} =         Execute          ozone sh bucket create /${PREFIX}-volume/${PREFIX}-bucket
                        Should not contain  ${output}       Failed

Create key
                        Execute and checkrc    echo "${PREFIX}: key created using Ozone Shell" > /tmp/sourcekey    0
    ${output} =         Execute          ozone sh key put /${PREFIX}-volume/${PREFIX}-bucket/${PREFIX}-key /tmp/sourcekey
                        Should not contain  ${output}       Failed
                        Execute and checkrc    rm /tmp/sourcekey    0

Create a bucket in s3v volume
    [Tags]    create-volume-and-bucket
    ${output} =         Execute          ozone sh bucket create /s3v/${PREFIX}-bucket
                        Should not contain  ${output}       Failed

Create key in the bucket in s3v volume
                        Execute and checkrc    echo "${PREFIX}: another key created using Ozone Shell" > /tmp/sourcekey    0
    ${output} =         Execute          ozone sh key put /s3v/${PREFIX}-bucket/key1-shell /tmp/sourcekey
                        Should not contain  ${output}       Failed
                        Execute and checkrc    rm /tmp/sourcekey    0

Try to create a bucket using S3 API
    [setup]             Setup v4 headers
    # Note: S3 API returns error if the bucket already exists
    ${random} =         Generate Ozone String
    ${output} =         Create bucket with name    ${PREFIX}-bucket-${random}
                        Should Be Equal    ${output}    ${None}

Create key using S3 API
                        Execute and checkrc    echo "${PREFIX}: key created using S3 API" > /tmp/sourcekey    0
    ${output} =         Execute AWSS3APICli and checkrc    put-object --bucket ${PREFIX}-bucket --key key2-s3api --body /tmp/sourcekey    0
                        Should not contain    ${output}    error
                        Execute and checkrc    rm /tmp/sourcekey    0
