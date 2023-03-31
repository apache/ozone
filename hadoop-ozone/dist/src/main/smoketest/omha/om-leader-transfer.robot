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

** Keywords ***
Get OM Leader Node
    ${result} =             Execute                 ozone admin om roles --service-id=omservice
                            LOG                     ${result}
                            Should Contain          ${result}               LEADER              1
                            Should Contain          ${result}               FOLLOWER            2
    ${omLine} =             Get Lines Containing String                     ${result}           LEADER
    ${split1}               ${split2}               Split String            ${omLine}           :
    ${leaderOM} =           Strip String            ${split1}
                            LOG                     Leader OM: ${leaderOM}
    [return]                ${leaderOM}

Get One OM Follower Node
    ${result} =             Execute                 ozone admin om roles --service-id=omservice
                            LOG                     ${result}
                            Should Contain          ${result}               LEADER              1
                            Should Contain          ${result}               FOLLOWER            2
    ${omLines} =            Get Lines Containing String                     ${result}           FOLLOWER
    ${omLine} =             Get Line                ${omLines}              0
    ${split1}               ${split2}               Split String            ${omLine}           :
    ${followerOM} =         Strip String            ${split1}
                            LOG                     Follower OM: ${followerOM}
    [return]                ${followerOM}


** Test Cases ***
Transfer Leadership for OM
    # Find Leader OM and one Follower OM
    ${leaderOM} =           Get OM Leader Node
                            LOG                     Leader OM: ${leaderOM}
    ${followerOM} =         Get One OM Follower Node
                            LOG                     Follower OM: ${followerOM}
    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer --service-id=omservice -n ${followerOM}
                            LOG                     ${result}
                            Should Contain          ${result}               Transfer leadership successfully

    ${newLeaderOM} =        Get OM Leader Node
                            Should be Equal         ${followerOM}           ${newLeaderOM}

Transfer Leadership for OM randomly
    # Find Leader OM and one Follower OM
    ${leaderOM} =           Get OM Leader Node
                            LOG                     Leader OM: ${leaderOM}
    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer -r
                            LOG                     ${result}
                            Should Contain          ${result}               Transfer leadership successfully

    ${newLeaderOM} =        Get OM Leader Node
                            Should Not be Equal     ${leaderOM}             ${newLeaderOM}
