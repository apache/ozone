# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Minimal AWS CLI smoke for packaged ozone local S3.
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup local ozone S3 smoke

*** Variables ***
${ENDPOINT_URL}       http://127.0.0.1:9878
${S3_ACCESS_KEY}      admin
${S3_SECRET_KEY}      admin123
${S3_REGION}          us-east-1

*** Keywords ***
Setup local ozone S3 smoke
    Install aws cli
    # The values the ozone local startup summary prints. Local Ozone runs without security, so
    # the S3 Gateway accepts any credentials; these keep bucket ownership stable across restarts.
    Execute                    aws configure set aws_access_key_id ${S3_ACCESS_KEY}
    Execute                    aws configure set aws_secret_access_key ${S3_SECRET_KEY}
    Execute                    aws configure set region ${S3_REGION}
    Execute                    aws configure set default.s3.signature_version s3v4
    Execute                    aws configure set default.s3.addressing_style path
    ${random} =                Generate Random String    8    [LOWER]
    Set Suite Variable         ${BUCKET}    bucket-${random}
    Create bucket with name    ${BUCKET}

*** Test Cases ***
AWS CLI can create list put and get against ozone local
    Execute                    echo "local ozone" > /tmp/local-ozone-smoke.txt
    ${result} =                Execute AWSS3APICli    list-buckets
    Should Contain             ${result}    ${BUCKET}
    ${result} =                Execute AWSS3Cli    cp /tmp/local-ozone-smoke.txt s3://${BUCKET}/smoke.txt
    Should Contain             ${result}    upload
    ${result} =                Execute AWSS3Cli    ls s3://${BUCKET}
    Should Contain             ${result}    smoke.txt
    Execute AWSS3APICli        get-object --bucket ${BUCKET} --key smoke.txt /tmp/local-ozone-smoke-copy.txt
    Compare files              /tmp/local-ozone-smoke.txt    /tmp/local-ozone-smoke-copy.txt
