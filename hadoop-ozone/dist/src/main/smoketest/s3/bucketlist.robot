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

List buckets
    ${result} =         Execute AWSS3APICli     list-buckets | jq -r '.Buckets[].Name'
                        Should contain          ${result}    ${BUCKET}

Get bucket info with Ozone Shell to check the owner field
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skipping this check as security is not enabled
    ${result} =         Execute             ozone sh bucket info /s3v/${BUCKET} | jq -r '.owner'
                        Should Be Equal     ${result}       testuser
                        # In ozonesecure(-ha) docker-config, hadoop.security.auth_to_local is set
                        # in the way that getShortUserName() converts the accessId to "testuser".
                        # Also see "Setup dummy credentials for S3" in commonawslib.robot

List buckets with empty access id
                        Execute                    aws configure set aws_access_key_id ''
    ${result} =         Execute AWSS3APICli and checkrc         list-buckets    255
                        Should contain            ${result}         The authorization header you provided is invalid