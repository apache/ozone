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
Library             BuiltIn
Resource            ./commonawslib.robot
Suite Setup         Setup v4 headers


*** Variables ***
${ENDPOINT_URL}         http://s3g:9878

*** Keywords ***
#   Export access key and secret to the environment
Setup aws credentials
    ${accessKey} =      Execute     aws configure get aws_access_key_id
    ${secret} =         Execute     aws configure get aws_secret_access_key
    Set Environment Variable        AWS_SECRET_ACCESS_KEY  ${secret}
    Set Environment Variable        AWS_ACCESS_KEY_ID  ${accessKey}

Freon S3BG
    [arguments]    ${prefix}=s3bg    ${n}=100    ${threads}=10   ${args}=${EMPTY}
    ${result} =        Execute          ozone freon s3bg -e ${ENDPOINT_URL} -t ${threads} -n ${n} -p ${prefix} ${args}
                       Should contain   ${result}       Successful executions: ${n}

*** Test Cases ***
Run Freon S3BG
    [Setup]    Setup aws credentials
    Freon S3BG