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
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***


*** Test Cases ***
Read data from previously created key
    ${random} =         Generate Random String  5  [NUMBERS]
    ${output} =         Execute          ozone sh key get /${PREFIX}-volume/${PREFIX}-bucket/${PREFIX}-key /tmp/key-${random}
                        Should not contain  ${output}       Failed
    ${output} =         Execute and checkrc    cat /tmp/key-${random}    0
                        Should contain    ${output}    ${PREFIX}: key created using Ozone Shell
                        Execute and checkrc    rm /tmp/key-${random}    0

Read key created with Ozone Shell using S3 API
    [setup]             Setup v4 headers
    ${output} =         Execute AWSS3APICli and checkrc    get-object --bucket ${PREFIX}-bucket --key key1-shell /tmp/get-result    0
                        Should contain    ${output}    "ContentLength"
    ${output} =         Execute and checkrc    cat /tmp/get-result    0
                        Should contain    ${output}    ${PREFIX}: another key created using Ozone Shell
                        Execute and checkrc    rm /tmp/get-result    0

Read key created with S3 API using S3 API
    [setup]             Setup v4 headers
    ${output} =         Execute AWSS3APICli and checkrc    get-object --bucket ${PREFIX}-bucket --key key2-s3api /tmp/get-result    0
                        Should contain    ${output}    "ContentLength"
    ${output} =         Execute and checkrc    cat /tmp/get-result    0
                        Should contain    ${output}    ${PREFIX}: key created using S3 API
                        Execute and checkrc    rm /tmp/get-result    0
