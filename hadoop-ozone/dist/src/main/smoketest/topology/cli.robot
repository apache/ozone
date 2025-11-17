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
Test Timeout        5 minutes

*** Variables ***

*** Keywords ***
Validate PrintTopology Output
    [Arguments]           ${output}
    Should Contain        ${output}         State = HEALTHY
    Should Contain        ${output}         IN_SERVICE
    Should Match Regexp   ${output}         .*ozone.*datanode[-_]\\d+.*IN_SERVICE.*rack.*


*** Test Cases ***
Run printTopology
    ${output} =         Execute          ozone admin printTopology
                        Validate PrintTopology Output    ${output}

Run printTopology -o
    ${output} =         Execute          ozone admin printTopology -o
                        Should Match Regexp   ${output}         Location: /.*rack.*
                        Should Match Regexp   ${output}         .*ozone.*datanode[-_]\\d+.*IN_SERVICE.*

Run printTopology --operational-state IN_SERVICE
    ${output} =         Execute          ozone admin printTopology --operational-state IN_SERVICE
                        Validate PrintTopology Output    ${output}

Run printTopology --node-state HEALTHY
    ${output} =         Execute          ozone admin printTopology --node-state HEALTHY
                        Validate PrintTopology Output    ${output}

