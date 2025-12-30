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
Documentation       Smoke test for ozone manager cancel prepare
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Run Keywords       Generate volume and bucket names
...    AND          Get Security Enabled From Config
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

** Keywords ***
Generate volume and bucket names
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${volume_name}  ${random}-volume-for-cancel
    Set Suite Variable  ${bucket_name}  ${random}-bucket-for-cancel

** Test Cases ***
Cancel Ozone Manager Prepare
    ${result} =        Execute      ozone admin om cancelprepare -id=omservice
    Wait Until Keyword Succeeds      3min   5sec    Should contain   ${result}   Cancel prepare succeeded

Test write operations
    Execute            ozone sh volume create /${volume_name}
    Execute            ozone sh bucket create /${volume_name}/${bucket_name}
    ${result} =        Execute   ozone sh key put /${volume_name}/${bucket_name}/cancel-key /opt/hadoop/NOTICE.txt
    ${result} =        Execute             ozone sh key info /${volume_name}/${bucket_name}/cancel-key
                       Should contain      ${result}       \"name\" : \"cancel-key\"

