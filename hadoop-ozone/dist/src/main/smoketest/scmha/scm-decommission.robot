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
Documentation       Test Ozone SCM Decommissioning
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Kinit test user     testuser     testuser.keytab

*** Variables ***
${VOLUME}           decom-volume
${BUCKET}           decom-bucket
${TESTFILE}         testfiledecomm

*** Keywords ***
Create volume bucket and put key
    Execute                 ozone sh volume create /${VOLUME}
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}
    Create File             /tmp/${TESTFILE}
    Execute                 echo "This is a decommissioning test" > /tmp/${TESTFILE}
    ${md5sum} =             Execute     md5sum /tmp/${TESTFILE} | awk '{print $1}'
    Execute                 ozone sh key put /${VOLUME}/${BUCKET}/${TESTFILE} /tmp/${TESTFILE}
    [Return]                ${md5sum}

Get Primordial SCM ID
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
    ${primordial_node} =    Get Lines Matching Pattern            ${result}         scm[1234].org:9894:LEADER*
    ${primordial_split} =   Split String            ${primordial_node}         :
    ${primordial_scmId} =   Strip String            ${primordial_split[3]}
    [Return]                ${primordial_scmId}

Get SCM Node count
    ${result} =              Execute                 ozone admin scm roles --service-id=scmservice
    ${nodes_in_quorum} =     Get Lines Matching Pattern                      ${result}           scm[1234].org:9894:*
    ${node_count} =          Get Line Count          ${nodes_in_quorum}
    [Return]                 ${node_count}


Transfer Leader to non-primordial node Follower
    ${result} =             Execute                 ozone admin scm roles --service-id=scmservice
                            LOG                     ${result}
    ${follower_nodes} =     Get Lines Matching Pattern                     ${result}       scm[1234].org:9894:FOLLOWER*
    ${follower_node} =      Get Line                ${follower_nodes}      0
    ${follower_split} =     Split String            ${follower_node}       :
    ${follower_scmId} =     Strip String            ${follower_split[3]}

    ${result} =             Execute                 ozone admin scm transfer --service-id=scmservice -n ${follower_scmId}
                            LOG                     ${result}
    [Return]                ${result}

*** Test Cases ***
Decommission SCM Primordial Node
    ${primordial_scm_id} =  Get Primordial SCM ID
                            LOG                     Primordial scm id : ${primordial_scm_id}
    ${decomm_output} =      Execute And Ignore Error   ozone admin scm decommission --nodeid=${primordial_scm_id}
                            LOG                     ${decomm_output}
                            Should Contain          ${decomm_output}               Cannot remove current leader
    ${md5sum} =             Create volume bucket and put key
    ${transfer_result} =    Transfer Leader to non-primordial node Follower
                            Should Contain          ${transfer_result}       Transfer leadership successfully
    ${node_count} =         Get SCM Node count
    ${node_count_pre} =     Convert to String       ${node_count}
    ${n} =                  Evaluate                ${node_count}-1
    ${node_count_expect} =  Convert to String       ${n}
                            LOG                     SCM Instance Count before SCM Decommission: ${node_count_pre}
    ${decommission_res} =   Execute                 ozone admin scm decommission --nodeid=${primordial_scm_id}
                            LOG                     ${decommission_res}
                            Should Contain          ${decommission_res}                         Decommissioned
    ${node_count} =         Get SCM Node count
    ${node_count_post} =    Convert to String       ${node_count}
                            LOG                     SCM Instance Count after SCM Decommission: ${node_count_post}
                            Should be Equal         ${node_count_expect}                       ${node_count_post}
    Execute                 ozone sh key get /${VOLUME}/${BUCKET}/${TESTFILE} /tmp/getdecomfile
    ${md5sum_new} =         Execute                 md5sum /tmp/getdecomfile | awk '{print $1}'
                            Should be Equal         ${md5sum}                      ${md5sum_new}
