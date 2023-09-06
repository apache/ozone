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
${TARGET_SCM}=      scm2.org

** Keywords ***
Get SCM Leader Node
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
                            Should Contain          ${result}               LEADER              1
                            Should Contain          ${result}               FOLLOWER            2
    ${scmLine} =            Get Lines Containing String                     ${result}           LEADER
    ${splits} =             Split String            ${scmLine}          :
    ${leaderSCM} =          Strip String            ${splits[3]}
                            LOG                     Leader SCM: ${leaderSCM}
    [return]                ${leaderSCM}

Get SCM UUID
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
    ${scm_line} =           Get Lines Containing String                     ${result}            ${TARGET_SCM}
    ${scm_split} =          Split String            ${scm_line}             :
    ${scm_uuid} =           Strip String            ${scm_split[3]}
    [return]                ${scm_uuid}

*** Test Cases ***
Transfer Leadership
    # Find Leader SCM
    ${leaderSCM} =          Get SCM Leader Node
                            LOG                     Leader SCM: ${leaderSCM}
    ${target_scm_uuid} =    Get SCM UUID
    # Transfer leadership to target SCM
    ${result} =             Execute                 ozone admin scm transfer --service-id=scmservice -n ${target_scm_uuid}
                            LOG                     ${result}
                            Should Contain          ${result}               Transfer leadership successfully

    ${newLeaderSCM} =       Get SCM Leader Node
                            Should Not be Equal     ${leaderSCM}            ${newLeaderSCM}