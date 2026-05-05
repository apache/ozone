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
Documentation       Smoke test for om fetch-key
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minute
Suite Setup         Kinit test user     testuser     testuser.keytab


*** Variables ***


*** Keywords ***


*** Test Cases ***
Fetch Key with Valid ServiceID Specified
    ${result} =              Execute                        ozone admin om fetch-key --service-id=omservice
    Should Contain           ${result}                      Current Secret Key ID

Fetch Key with Multiple ServiceIDs, Valid ServiceID Specified
    ${result} =              Execute                        ozone admin --set=ozone.om.service.ids=omservice,omservice2 om fetch-key --service-id=omservice
    Should Contain           ${result}                      Current Secret Key ID

Fetch Key with Multiple ServiceIDs, Unconfigured ServiceID Specified
    ${result} =              Execute And Ignore Error       ozone admin --set=ozone.om.service.ids=omservice,omservice2 om fetch-key --service-id=omservice3
    Should Contain           ${result}                      Service ID specified does not match

Fetch Key with Multiple ServiceIDs, Invalid ServiceID Specified
    ${result} =              Execute And Ignore Error       ozone admin --set=ozone.om.service.ids=omservice,omservice2 om fetch-key --service-id=omservice2
    Should Contain           ${result}                      Could not find any configured addresses for OM.

Fetch Key without OM Service ID
    ${result} =              Execute                        ozone admin om fetch-key
    Should Contain           ${result}                      Current Secret Key ID

Fetch Key with Multiple ServiceIDs, No ServiceID Specified
    ${result} =              Execute And Ignore Error       ozone admin --set=ozone.om.service.ids=omservice,ozone1 om fetch-key
    Should Contain           ${result}                      no Ozone Manager service ID specified
