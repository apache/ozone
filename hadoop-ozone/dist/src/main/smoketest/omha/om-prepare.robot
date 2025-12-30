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
Documentation       Smoke test to test preparing OMs in an OM HA cluster.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes
Suite Setup         Run Keywords       Get Security Enabled From Config
...                 AND                Create Specific OM data for prepare
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Keywords ***
Create Specific OM data for prepare
    # Freon data to make sure there are a reasonable number of transactions in the system.
    Freon OCKG    prefix=om-prepare    n=100
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${volume_name}  ${random}-volume-for-prepare
    Set Suite Variable  ${bucket_name}  ${random}-bucket-for-prepare
    Execute             ozone sh volume create /${volume_name}
    Execute             ozone sh bucket create /${volume_name}/${bucket_name}
    Execute             ozone sh key put /${volume_name}/${bucket_name}/prepare-key /opt/hadoop/NOTICE.txt

** Test Cases ***
Prepare Ozone Manager
    ${result} =        Execute      ozone admin om prepare -id=omservice
                       Wait Until Keyword Succeeds      3min       10sec     Should contain   ${result}   OM Preparation successful!

Checks if the expected data is present in OM
    ${result} =         Execute             ozone sh key info /${volume_name}/${bucket_name}/prepare-key
                        Should contain      ${result}       \"name\" : \"prepare-key\"

Test write operation fails
    ${result} =        Execute and checkrc    ozone sh key put /${volume_name}/${bucket_name}/prepare-key2 /opt/hadoop/NOTICE.txt    255
                       Should contain         ${result}       OM is in prepare mode
                       Execute and checkrc    ozone sh key info /${volume_name}/${bucket_name}/prepare-key2    255
