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
Documentation       S3 gateway test with rclone client
Library             OperatingSystem
Library             BuiltIn
Resource            ./commonawslib.robot
Test Timeout        15 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}             http://s3g:9878
${S3_VOLUME}                s3v
${BUCKET}                   generated
${RCLONE_CONFIG_NAME}       ozone
${RCLONE_CONFIG_PATH}       /tmp/rclone.conf
${RCLONE_VERBOSE_LEVEL}     2

*** Keywords ***
#   Export access key and secret to the environment
Setup aws credentials
    ${accessKey} =      Execute     aws configure get aws_access_key_id
    ${secret} =         Execute     aws configure get aws_secret_access_key
    Set Environment Variable        AWS_SECRET_ACCESS_KEY  ${secret}
    Set Environment Variable        AWS_ACCESS_KEY_ID  ${accessKey}

*** Test Cases ***
Rclone Client Test
    [Setup]    Setup aws credentials
    Set Environment Variable   RCLONE_CONFIG    ${RCLONE_CONFIG_PATH}
    Set Environment Variable   RCLONE_VERBOSE   ${RCLONE_VERBOSE_LEVEL}
    ${result} =     Execute    rclone config create ${RCLONE_CONFIG_NAME} s3 env_auth=true provider=Other endpoint=${ENDPOINT_URL}
    ${result} =     Execute    rclone copy /opt/hadoop/smoketest ${RCLONE_CONFIG_NAME}:/${S3_VOLUME}/${BUCKET}
