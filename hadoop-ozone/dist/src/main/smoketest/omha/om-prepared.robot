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
Documentation       Smoke test to test that OMs are prepared in an OM HA cluster.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

** Test Cases ***
Test create volume fails
    ${random} =         Generate Random String    5    [NUMBERS]
                        Set Suite Variable    ${volume_name}    ${random}-volume-for-prepare
    ${result} =         Execute and checkrc    ozone sh volume create /${volume_name}    255
                        Should contain         ${result}       OM is in prepare mode
    ${result} =         Execute and checkrc    ozone sh volume info /${volume_name}    255
                        Should contain         ${result}       VOLUME_NOT_FOUND

Test list volumes succeeds
    ${result} =    Execute    ozone sh volume list

