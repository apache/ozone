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


*** Variables ***
${RULE1}    /opt/hadoop/share/ozone/byteman/skip-put-block.btm
${RULE2}    /opt/hadoop/share/ozone/byteman/skip-notify-group-remove.btm

*** Settings ***
Resource            BytemanKeywords.robot


*** Test Cases ***

Print All Byteman Rules
    Inject Fault Into All Components      ${RULE1}
    List Byteman Rules for All Components
    Remove Fault From All Components      ${RULE1}

Inject Byteman Rule in one component
    Add Byteman Rule       datanode1    ${RULE2}
    List Byteman Rules     datanode1
    Remove Byteman Rule    datanode1    ${RULE2}

Inject Multiple Byteman Rules in one component
    Add Byteman Rule             datanode1    ${RULE1}
    Add Byteman Rule             datanode1    ${RULE2}
    ${rules} =    List Byteman Rules           datanode1
    ${rules_count} =    Get Length    ${rules}
    Should Be Equal As Integers    ${rules_count}    2
    Remove All Byteman Rules                   datanode1
    ${rules} =    List Byteman Rules           datanode1
    Should Be Empty    ${rules}

Test Datanode Only Fault Injection
    Inject Fault Into Datanodes Only    ${RULE1}
    List Byteman Rules for Datanodes
    Remove Fault From Datanodes Only    ${RULE1}

Test OM Only Fault Injection
    Inject Fault Into OMs Only          ${RULE1}
    List Byteman Rules for OMs
    Remove Fault From OMs Only          ${RULE1}

Test SCM Only Fault Injection
    Inject Fault Into SCMs Only         ${RULE1}
    List Byteman Rules for SCMs
    Remove Fault From SCMs Only         ${RULE1}