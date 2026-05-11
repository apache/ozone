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
Documentation       S3 gateway test with aws cli for bucket lifecycle
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

Set bucket lifecycle configuration
    ${bucket} =         Create bucket
    ${lifecycle_json} =     Set Variable    {"Rules": [{"ID": "Rule1", "Prefix": "prefix1/", "Status": "Enabled", "Expiration": {"Days": 1}}]}
    ${result} =         Execute AWSS3APICli     put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '${lifecycle_json}'
                        Should Be Empty         ${result}

Get bucket lifecycle configuration
    ${bucket} =         Create bucket
    ${lifecycle_json} =     Set Variable    {"Rules": [{"ID": "Rule1", "Prefix": "prefix1/", "Status": "Enabled", "Expiration": {"Days": 1}}]}
    ${result} =         Execute AWSS3APICli     put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '${lifecycle_json}'
    ${result} =         Execute AWSS3APICli     get-bucket-lifecycle-configuration --bucket ${bucket}
                        Should contain          ${result}           Rule1
                        Should contain          ${result}           prefix1/
                        Should contain          ${result}           Enabled

Delete bucket lifecycle configuration
    ${bucket} =         Create bucket
    ${lifecycle_json} =     Set Variable    {"Rules": [{"ID": "Rule1", "Prefix": "prefix1/", "Status": "Enabled", "Expiration": {"Days": 1}}]}
    ${result} =         Execute AWSS3APICli     put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '${lifecycle_json}'
    ${result} =         Execute AWSS3APICli     delete-bucket-lifecycle --bucket ${bucket}
                        Should Be Empty         ${result}
    ${result} =         Execute AWSS3APICli and checkrc     get-bucket-lifecycle-configuration --bucket ${bucket}    255
                        Should contain          ${result}           NoSuchLifecycleConfiguration
