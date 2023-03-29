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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***

** Keywords ***
Get SCM Node Count
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
    ${scm_count} =          Get Line Count          ${result}
    [return]                ${scm_count}

*** Test Cases ***
Verify SCM Count
    ${scm_count} =          Get SCM Node Count
                            LOG                     SCM Instance Count: ${scm_count}
    ${scm_count} =          Convert To String       ${scm_count}
                            Should be Equal         4                       ${scm_count}

Transfer Leader to SCM4
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
    ${scm4_line} =          Get Lines Containing String                      ${result}           scm4.org
    ${scm4_split} =         Split String            ${scm4_line}             :
    ${scm4_uuid} =          Strip String            ${scm4_split[3]}

    ${result} =             Execute                 ozone admin scm transfer --service-id=scmservice -n ${scm4_uuid}
                            LOG                     ${result}
                            Should Contain          ${result}                Transfer leadership successfully


