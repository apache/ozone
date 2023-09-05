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

*** Test Cases ***
Transfer Leader to non-primordial node Follower
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
    ${follower_nodes} =     Get Lines Matching Pattern                      ${result}           scm[23].org:9894:FOLLOWER*
    ${follower_node} =      Get Line                ${follower_nodes}        0
    ${follower_split} =     Split String            ${follower_node}         :
    ${follower_scmId} =     Strip String            ${follower_split[3]}

    ${result} =             Execute                 ozone admin scm transfer --service-id=scmservice -n ${follower_scmId}
                            LOG                     ${result}
                            Should Contain          ${result}                Transfer leadership successfully

Decommission SCM Primordial Node
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
    ${nodes_in_quorum} =    Get Lines Matching Pattern                      ${result}           scm[1234].org:9894:*
    ${node_count} =         Get Line Count          ${nodes_in_quorum}
    ${node_count_pre} =     Convert to String       ${node_count}
    ${n} =                  Evaluate                ${node_count}-1
    ${node_count_expect} =  Convert to String       ${n}
                            LOG                     SCM Instance Count before SCM Decommission: ${node_count_pre}
    ${primordial_node} =    Get Lines Containing String                     ${result}           scm1
    ${primordial_split} =   Split String            ${primordial_node}      :
    ${primordial_scmId} =   Strip String            ${primordial_split[3]}
    ${decommission_res} =   Execute                 ozone admin scm decommission --nodeid=${primordial_scmId}
                            LOG                     ${decommission_res}
                            Should Contain          ${decommission_res}                         Decommissioned
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
    ${nodes_in_quorum} =    Get Lines Matching Pattern                      ${result}           scm[1234].org:9894:*
    ${node_count} =         Get Line Count          ${nodes_in_quorum}
    ${node_count_post} =    Convert to String       ${node_count}
                            LOG                     SCM Instance Count after SCM Decommission: ${node_count_post}
                            Should be Equal         ${node_count_expect}                        ${node_count_post}

