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
Documentation       Smoketest for OM leader transfer
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes


*** Keywords ***
Get OM Nodes
    ${result} =        Execute                 ozone admin om roles --service-id=omservice --json | jq -r '[.[] | .[] | select(.serverRole == "LEADER") | .hostname], [.[] | .[] | select(.serverRole == "FOLLOWER") | .hostname] | .[]'
    ${leader}=         Get Line                ${result}    0
    ${follower1}=      Get Line                ${result}    1
    ${follower2}=      Get Line                ${result}    2
    [Return]           ${leader}    ${follower1}    ${follower2}

Get OM Leader Node
    ${result} =        Get OM Nodes
    [Return]           ${result}[0]

Get One OM Follower Node
    ${result} =        Get OM Nodes
    [Return]           ${result}[1]

Get OM Leader and One Follower Node
    ${result} =        Get OM Nodes
    [Return]           ${result}[0]      ${result}[1]

Assert OM leader Role Transitions
    [arguments]             ${leaderOM}     ${followerOM}     ${isEqualCheck}
    ${newLeaderOM} =        Get OM Leader Node
                            Should not be Equal     ${leaderOM}             ${newLeaderOM}
    Run Keyword If          '${isEqualCheck}' == 'true'         Should be Equal    ${followerOM}    ${newLeaderOM}


*** Test Cases ***
Transfer Leadership for OM with Valid ServiceID Specified
    # Find Leader OM and one Follower OM
    ${leaderOM}  ${followerOM} =  Get OM Leader and One Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer --service-id=omservice -n ${followerOM}
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ${followerOM}   true

Transfer Leadership for OM with Multiple ServiceIDs, Valid ServiceID Specified
    # Find Leader OM and one Follower OM
    ${leaderOM}  ${followerOM} =  Get OM Leader and One Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice -n ${followerOM}
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ${followerOM}   true

Transfer Leadership for OM with Multiple ServiceIDs, Unconfigured ServiceID Specified
    # Find one Follower OM
    ${followerOM} =         Get One OM Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute And Ignore Error                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice3 -n ${followerOM}
                            Should Contain          ${result}        Service ID specified does not match

Transfer Leadership for OM with Multiple ServiceIDs, Invalid ServiceID Specified
    # Find one Follower OM
    ${followerOM} =         Get One OM Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute And Ignore Error                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice2 -n ${followerOM}
                            Should Contain          ${result}        Could not find any configured addresses for OM.

Transfer Leadership for OM without ServiceID specified
    # Find Leader OM and one Follower OM
    ${leaderOM}  ${followerOM} =  Get OM Leader and One Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer -n ${followerOM}
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ${followerOM}   true

Transfer Leadership for OM with Multiple ServiceIDs, No ServiceID Specified
    # Find one Follower OM
    ${followerOM} =         Get One OM Follower Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute And Ignore Error                 ozone admin --set=ozone.om.service.ids=omservice,ozone1 om transfer -n ${followerOM}
                            Should Contain           ${result}       no Ozone Manager service ID specified

Transfer Leadership for OM randomly with Valid ServiceID Specified
    # Find Leader OM and one Follower OM
    ${leaderOM} =           Get OM Leader Node
    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer --service-id=omservice -r
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ""   false

Transfer Leadership for OM randomly with Multiple ServiceIDs, Valid ServiceID Specified
    # Find Leader OM and one Follower OM
    ${leaderOM} =           Get OM Leader Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice -r
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ""   false

Transfer Leadership for OM randomly with Multiple ServiceIDs, Unconfigured ServiceID Specified
    # Transfer leadership to the Follower OM
    ${result} =             Execute And Ignore Error                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice3 -r
                            Should Contain           ${result}       Service ID specified does not match

Transfer Leadership for OM randomly with Multiple ServiceIDs, Invalid ServiceID Specified
    # Transfer leadership to the Follower OM

    ${result} =             Execute And Ignore Error                 ozone admin --set=ozone.om.service.ids=omservice,omservice2 om transfer --service-id=omservice2 -r
                            Should Contain           ${result}       Could not find any configured addresses for OM.

Transfer Leadership for OM randomly without ServiceID specified
    # Find Leader OM and one Follower OM
    ${leaderOM} =           Get OM Leader Node

    # Transfer leadership to the Follower OM
    ${result} =             Execute                 ozone admin om transfer -r
                            Should Contain          ${result}               Transfer leadership successfully

    Assert OM Leader Role Transitions    ${leaderOM}   ""   false

Transfer Leadership for OM randomly with Multiple ServiceIDs, No ServiceID Specified
    # Transfer leadership to the Follower OM

    ${result} =             Execute And Ignore Error                  ozone admin --set=ozone.om.service.ids=omservice,ozone1 om transfer -r
                            Should Contain           ${result}        no Ozone Manager service ID specified
