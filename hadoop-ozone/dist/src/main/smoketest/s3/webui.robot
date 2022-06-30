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
Documentation       S3 gateway web ui test
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ./commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests
Default Tags        no-bucket-type

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

S3 Gateway Web UI
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -v ${ENDPOINT_URL}
                        Should contain      ${result}       Location
                        Should contain      ${result}       /static/
    ${result} =         Execute                             curl --negotiate -u : -v ${ENDPOINT_URL}/static/index.html
                        Should contain      ${result}       Apache Ozone S3
