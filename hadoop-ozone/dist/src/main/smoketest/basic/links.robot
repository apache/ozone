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
Documentation       Test bucket links via Ozone CLI
Library             OperatingSystem
Resource            ../commonlib.robot
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        2 minute
Suite Setup         Create volumes

*** Variables ***
${prefix}    generated

*** Keywords ***
Create volumes
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${source}  ${random}-source
    Set Suite Variable  ${target}  ${random}-target
    Execute             ozone sh volume create ${source}
    Execute             ozone sh volume create ${target}

*** Test Cases ***
Link to non-existent bucket
                        Execute                     ozone sh bucket link ${source}/no-such-bucket ${target}/dangling-link
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/dangling-link
                        Should contain              ${result}         BUCKET_NOT_FOUND

Key create passthrough
                        Execute                     ozone sh bucket link ${source}/bucket1 ${target}/link1
                        Execute                     ozone sh bucket create ${source}/bucket1
                        Execute                     ozone sh key put ${target}/link1/key1 /etc/passwd
                        Compare key to local file   ${target}/link1/key1    /etc/passwd

Key read passthrough
                        Execute                     ozone sh key put ${source}/bucket1/key2 /opt/hadoop/NOTICE.txt
                        Compare key to local file   ${source}/bucket1/key2    /opt/hadoop/NOTICE.txt

Key list passthrough
    ${target_list} =    Execute                     ozone sh key list ${target}/link1 | jq -r '.name'
    ${source_list} =    Execute                     ozone sh key list ${source}/bucket1 | jq -r '.name'
                        Should Be Equal             ${target_list}    ${source_list}
                        Should Contain              ${source_list}    key1
                        Should Contain              ${source_list}    key2

