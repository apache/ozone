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
Library             OperatingSystem
Library             String
Library             BuiltIn

Resource            lib/os.robot

*** Variables ***
${SECURITY_ENABLED}  ${EMPTY}
${OM_HA_PARAM}       ${EMPTY}
${OM_SERVICE_ID}     om

*** Keywords ***
Get test user principal
    [arguments]         ${user}
    ${instance} =       Execute                    hostname | sed 's/scm[0-9].org/scm/;s/scm[0-9]/scm/;s/om[0-9]/om/'
    [return]            ${user}/${instance}@EXAMPLE.COM

Get Security Enabled From Config
    Return From Keyword If    '${SECURITY_ENABLED}' != ''
    ${value} =    Execute    ozone getconf confKey ozone.security.enabled
    IF    '${value}' != 'true' and '${value}' != 'false'
           ${value} =    Set Variable    false
    END
    Set Global Variable      ${SECURITY_ENABLED}     ${value}

Kinit HTTP user
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    ${principal} =      Get test user principal    HTTP
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k -t /etc/security/keytabs/HTTP.keytab ${principal}

Kinit test user
    Get Security Enabled From Config
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skip in unsecure cluster
    [arguments]                      ${user}       ${keytab}
    ${TEST_USER} =      Get test user principal    ${user}
    Set Suite Variable  ${TEST_USER}
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k -t /etc/security/keytabs/${keytab} ${TEST_USER}

Access should be denied
    [arguments]    ${command}
    ${output} =         Execute And Ignore Error     ${command}
                        Should contain   ${output}   Access denied

Requires admin privilege
    [arguments]    ${command}
    Pass Execution If   '${SECURITY_ENABLED}' == 'false'    Skip privilege check in unsecure cluster
    Kinit test user     testuser2     testuser2.keytab
    Access should be denied    ${command}
