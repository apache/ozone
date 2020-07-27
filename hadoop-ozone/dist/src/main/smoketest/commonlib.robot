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
${SECURITY_ENABLED}  false
${OM_HA_PARAM}       ${EMPTY}
${OM_SERVICE_ID}     om

*** Keywords ***
Kinit HTTP user
    ${hostname} =       Execute                    hostname
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k HTTP/${hostname}@EXAMPLE.COM -t /etc/security/keytabs/HTTP.keytab

Kinit test user
    [arguments]                      ${user}       ${keytab}
    ${hostname} =       Execute                    hostname
    Set Suite Variable  ${TEST_USER}               ${user}/${hostname}@EXAMPLE.COM
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k ${user}/${hostname}@EXAMPLE.COM -t /etc/security/keytabs/${keytab}
