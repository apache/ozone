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
Documentation       Test ozone admin cert command
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config

*** Keywords ***
Setup Test
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Test Cases ***
List certificates
    Pass Execution If       '${SECURITY_ENABLED}' == 'false'    N/A
    ${output} =             Execute          ozone admin cert list
    Should Contain          ${output}        Certificate list:(BatchSize=

List certificates as JSON
    Pass Execution If      '${SECURITY_ENABLED}' == 'false'    N/A
    Execute                 ozone admin cert list --json 1>> ${TEMP_DIR}/outStream 2>> ${TEMP_DIR}/errStream
    ${output}               Execute             cat ${TEMP_DIR}/outStream | jq -r '.[0] | keys'
                            Should Contain          ${output}           serialNumber
    ${errOutput} =          Execute                 cat ${TEMP_DIR}/errStream
                            Should Contain          ${errOutput}        Certificate list:(BatchSize=
    [teardown]              Remove Files            ${TEMP_DIR}/outStream    ${TEMP_DIR}/errStream
