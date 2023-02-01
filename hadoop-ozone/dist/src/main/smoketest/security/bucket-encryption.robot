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
Documentation       Test for bucket encryption
Library             BuiltIn
Library             String
Resource            ../commonlib.robot
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Suite Setup         Setup Test
Test Timeout        5 minutes

*** Variables ***
${KEY_NAME}    key1
${VOLUME}

*** Keywords ***
Setup Test
    ${volume} =      Create Random Volume
    Set Suite Variable    ${VOLUME}    ${volume}


*** Test Cases ***
Create Encrypted Bucket
    ${output} =      Execute    ozone sh bucket create -k ${KEY_NAME} o3://${OM_SERVICE_ID}/${VOLUME}/encrypted-bucket
                     Should Not Contain    ${output}    INVALID_REQUEST
    Bucket Exists    o3://${OM_SERVICE_ID}/${VOLUME}/encrypted-bucket

Create Key in Encrypted Bucket
    ${key} =         Set Variable    o3://${OM_SERVICE_ID}/${VOLUME}/encrypted-bucket/passwd
    ${output} =      Execute    ozone sh key put ${key} /etc/passwd
    Key Should Match Local File    ${key}    /etc/passwd
