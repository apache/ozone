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
Documentation       Test ozone debug kerberos commands
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Test Cases ***
Diagnose Kerberos
    [Documentation]    Exercises the ozone debug kerberos diagnose command.
    ${output} =        Execute    ozone debug kerberos diagnose
    Should Contain     ${output}    == Ozone Kerberos Diagnostics ==
    Should Contain     ${output}    [PASS] Kerberos Configuration
    Should Contain     ${output}    [PASS] Kerberos kinit Command
    Should Contain     ${output}    [PASS] Auth-to-Local Mapping
    Should Contain     ${output}    == Diagnostic Summary ==

Translate Principal
    [Documentation]    Exercises the ozone debug kerberos translate-principal command for single principals.
    [Template]         Verify Principal Translation
    # PRINCIPAL                    # EXPECTED_SHORT_NAME
    om/om@EXAMPLE.COM              om
    scm/scm@EXAMPLE.COM            scm
    dn/dn@EXAMPLE.COM              dn

Translate Multiple Principals At Once
    [Documentation]    Exercises translation of multiple principals passed in a single command.
    ${output} =        Execute    ozone debug kerberos translate-principal testuser/om@EXAMPLE.COM om/om@EXAMPLE.COM
    Should Contain     ${output}    [PASS] testuser/om@EXAMPLE.COM
    Should Contain     ${output}    [PASS] om/om@EXAMPLE.COM
    Should Contain     ${output}    PASS : 2

Translate Invalid Principal
    [Documentation]    Ensures the command correctly flags principals that have no mapping rules and returns exit code 1.
    ${rc}  ${output} =  Run And Return Rc And Output    ozone debug kerberos translate-principal om/om@cloudera.com
    Should Be Equal As Integers    ${rc}    1
    Should Contain      ${output}    [FAIL] om/om@cloudera.com
    Should Contain      ${output}    No rules applied to


*** Keywords ***
Verify Principal Translation
    [Arguments]    ${principal}    ${expected_short}
    ${output} =    Execute         ozone debug kerberos translate-principal ${principal}
    Should Contain    ${output}    ${expected_short}
    Should Contain    ${output}    PASS : 1
