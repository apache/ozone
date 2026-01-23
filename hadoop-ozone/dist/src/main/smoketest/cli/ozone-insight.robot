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
Documentation       Test ozone insight command with HTTP/HTTPS and SPNEGO
Library             BuiltIn
Resource            ../lib/os.robot
Resource            ../commonlib.robot
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Keywords ***
Should Not Have Connection Errors
    [Arguments]    ${output}
    Should Not Contain    ${output}    Connection Refused
    Should Not Contain    ${output}    UnknownHostException
    Should Not Contain    ${output}    Authentication Failed
    Should Not Contain    ${output}    0.0.0.0

*** Test Cases ***
List Insight Points
    ${output} =    Execute    ozone insight list
    Should Contain    ${output}    scm.node-manager
    Should Contain    ${output}    om.key-manager
    Should Contain    ${output}    datanode.dispatcher

Test SCM Metrics Retrieval
    ${output} =    Execute    ozone insight metrics scm.node-manager
    
    Should Contain    ${output}    Metrics for `scm.node-manager`
    Should Contain    ${output}    Node counters
    Should Contain    ${output}    HB processing stats
    Should Not Have Connection Errors    ${output}

Test OM Metrics Retrieval
    ${output} =    Execute    ozone insight metrics om.key-manager
    
    Should Contain    ${output}    Metrics for `om.key-manager`
    Should Contain    ${output}    Key related metrics
    Should Contain    ${output}    Key operation stats
    Should Not Have Connection Errors    ${output}

Test SCM Log Streaming
    ${output} =    Execute And Ignore Error    timeout 10 ozone insight log scm.node-manager 2>&1 || true
    
    Should Contain Any    ${output}    [SCM]    SCMNodeManager
    Should Not Have Connection Errors    ${output}
