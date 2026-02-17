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
Documentation       Prepares OMs
Resource            ../commonlib.robot
Resource            lib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Test Cases ***
Prepare Ozone Manager
    Pass Execution If    '%{OZONE_UPGRADE_FROM}' == '1.1.0'    OM prepare is skipped for version %{OZONE_UPGRADE_FROM}
    ${service_id} =      Get OM Service ID
    Pass Execution If    '${service_id}' == ''    OM prepare skipped in non-HA
    Wait Until Keyword Succeeds      3min       10sec     Prepare OM

