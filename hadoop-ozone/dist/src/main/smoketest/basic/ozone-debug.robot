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
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Resource            ../commonlib.robot
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        2 minute
Suite Setup         Generate prefix

*** Variables ***
${prefix}    generated

*** Keywords ***
Generate prefix
   ${random} =         Generate Random String  5  [NUMBERS]
   Set Suite Variable  ${prefix}  ${random}

*** Test Cases ***
RpcClient with port
   Test ozone debug       o3://            om:9862     ${prefix}-rpcwoport

*** Keywords ***
Test ozone debug
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute             ozone sh volume create ${protocol}${server}/${volume} --quota 100TB
                    Should not contain  ${result}       Failed
                    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh key put ${protocol}${server}/${volume}/bb1/key1 /opt/hadoop/NOTICE.txt
                    Execute             rm -f /tmp/NOTICE.txt.1
    ${result} =     Execute             ozone debug chunkinfo ${protocol}${server}/${volume}/bb1/key1 | jq -r '.[]'
                    Should contain      ${result}       files
    ${result} =     Execute             ozone debug chunkinfo ${protocol}${server}/${volume}/bb1/key1 | jq -r '.[].files[0]'
    ${result3} =    Execute             echo "exists"
    ${result2} =    Execute             test -f ${result} && echo "exists"
                    Should Be Equal     ${result2}       ${result3}