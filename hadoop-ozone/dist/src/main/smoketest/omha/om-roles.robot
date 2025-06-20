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
Documentation       Smoke test for listing om roles.
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Keywords ***
Assert Leader Present
     [Arguments]                     ${output}
     Should Match Regexp             ${output}                      [om (: LEADER|)]

Assert Leader Present in JSON
     [Arguments]                     ${output}
     ${leader} =                     Execute                        echo '${output}' | jq '.[] | select(.[] | .serverRole == "LEADER")'
                                     Should Not Be Equal            ${leader}       ${EMPTY}
Assert Leader Present in TABLE
     [Arguments]                     ${output}
     Should Match Regexp             ${output}                      \\|.*LEADER.*

*** Test Cases ***
List om roles with OM service ID passed
    ${output_with_id_passed} =      Execute                         ozone admin om roles --service-id=omservice
                                    Assert Leader Present           ${output_with_id_passed}
    ${output_with_id_passed} =      Execute                         ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles --service-id=omservice
                                    Assert Leader Present           ${output_with_id_passed}

List om roles without OM service ID passed
    ${output_without_id_passed} =   Execute                         ozone admin om roles
                                    Assert Leader Present           ${output_without_id_passed}
    ${output_without_id_passed} =   Execute And Ignore Error        ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles
                                    Should Contain                  ${output_without_id_passed}      no Ozone Manager service ID specified

List om roles as JSON with OM service ID passed
    ${output_with_id_passed} =      Execute                         ozone admin om roles --service-id=omservice --json
                                    Assert Leader Present in JSON   ${output_with_id_passed}
    ${output_with_id_passed} =      Execute                         ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles --service-id=omservice --json
                                    Assert Leader Present in JSON   ${output_with_id_passed}

List om roles as JSON without OM service ID passed
    ${output_without_id_passed} =   Execute                         ozone admin om roles --json
                                    Assert Leader Present in JSON   ${output_without_id_passed}
    ${output_without_id_passed} =   Execute And Ignore Error        ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles --json
                                    Should Contain                  ${output_without_id_passed}      no Ozone Manager service ID specified

List om roles as TABLE with OM service ID passed
    ${output_with_id_passed} =      Execute                         ozone admin om roles --service-id=omservice --table
                                    Assert Leader Present in TABLE  ${output_with_id_passed}
    ${output_with_id_passed} =      Execute                         ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles --service-id=omservice --table
                                    Assert Leader Present in TABLE  ${output_with_id_passed}

List om roles as TABLE without OM service ID passed
    ${output_without_id_passed} =   Execute                         ozone admin om roles --table
                                    Assert Leader Present in TABLE  ${output_without_id_passed}
    ${output_without_id_passed} =   Execute And Ignore Error        ozone admin --set=ozone.om.service.ids=omservice,omservice2 om roles --table
                                    Should Contain                  ${output_without_id_passed}      no Ozone Manager service ID specified
