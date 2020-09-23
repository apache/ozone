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
Documentation       Test ozone admin container command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Create test data

*** Variables ***
${CONTAINER}

*** Keywords ***
Create test data
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
                        Execute          ozone freon ockg -n1 -t1 -p container

*** Test Cases ***
Create container
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${container} =      Execute          echo "${output}" | grep 'is created' | cut -f2 -d' '
                        Set Suite Variable    ${CONTAINER}    ${container}

List containers
    ${output} =         Execute          ozone admin container list
                        Should contain   ${output}   OPEN

List containers with explicit host
    ${output} =         Execute          ozone admin container list --scm scm
                        Should contain   ${output}   OPEN

Container info
    ${output} =         Execute          ozone admin container info "${CONTAINER}"
                        Should contain   ${output}   Container id: ${CONTAINER}
                        Should contain   ${output}   Pipeline id
                        Should contain   ${output}   Datanodes

Verbose container info
    ${output} =         Execute          ozone admin --verbose container info "${CONTAINER}"
                        Should contain   ${output}   Pipeline Info

Close container
                        Execute          ozone admin container close "${CONTAINER}"
    ${output} =         Execute          ozone admin container info "${CONTAINER}"
                        Should contain   ${output}   CLOS

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin container
                        Should contain   ${output}   Incomplete command
                        Should contain   ${output}   list
                        Should contain   ${output}   info
                        Should contain   ${output}   delete
                        Should contain   ${output}   create
                        Should contain   ${output}   close

List containers on unknown host
    ${output} =         Execute And Ignore Error     ozone admin --verbose container list --scm unknown-host
                        Should contain   ${output}   Invalid host name

