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
Documentation       Test ozone Debug CLI
Library             Collections
Resource            ../commonlib.robot

*** Variables ***
${VOLUME}           vol1
${BUCKET}           bucket1
${TESTFILE}         testfile

*** Keywords ***
Compare JSON
    [arguments]                     ${json}
    Should Be Equal                 ${json}[filename]                   ${VOLUME}/${BUCKET}/${TESTFILE}
    ${file_size} =                  Get File Size                       ${TESTFILE}
    Should Be Equal                 ${json}[datasize]                   ${file_size}
    Should Be Equal As Integers     ${json}[blocks][0][blockIndex]      1
    Should Not Be Empty             Convert To String       ${json}[blocks][0][containerId]
    Should Not Be Empty             Convert To String       ${json}[blocks][0][localId]
    Should Be Equal As Integers     ${json}[blocks][0][length]          1048576
    Should Not Be Empty             Convert To String       ${json}[blocks][0][offset]
    ${datanodes_b1} =               Create List   ${json}[blocks][0][replicas][0][hostname]    ${json}[blocks][0][replicas][1][hostname]   ${json}[blocks][0][replicas][2][hostname]
    ${datanodes_expected_b1} =      Create List  ozone_datanode_1.ozone_default  ozone_datanode_2.ozone_default  ozone_datanode_3.ozone_default
    Lists Should Be Equal	        ${datanodes_b1}    ${datanodes_expected_b1}   ignore_order=True
    Should Be Equal As Integers     ${json}[blocks][1][blockIndex]      2
    Should Not Be Empty             Convert To String       ${json}[blocks][1][containerId]
    Should Not Be Empty             Convert To String       ${json}[blocks][1][localId]
    Should Be Equal As Integers     ${json}[blocks][1][length]          451424
    Should Not Be Empty             Convert To String       ${json}[blocks][1][offset]
    ${datanodes_b2} =               Create List   ${json}[blocks][1][replicas][0][hostname]    ${json}[blocks][1][replicas][1][hostname]   ${json}[blocks][1][replicas][2][hostname]
    ${datanodes_expected_b2} =      Create List  ozone_datanode_1.ozone_default  ozone_datanode_2.ozone_default  ozone_datanode_3.ozone_default
    Lists Should Be Equal	        ${datanodes_b2}    ${datanodes_expected_b2}   ignore_order=True

Check cheksum mismatch error
    [arguments]                     ${json}     ${datanode}
    ${datanodes} =                  Create List     ${json}[blocks][0][replicas][0][hostname]   ${json}[blocks][0][replicas][1][hostname]   ${json}[blocks][0][replicas][2][hostname]
    ${index} =                      Get Index From List         ${datanodes}        ${datanode}
    Should Contain                  ${json}[blocks][0][replicas][${index}][exception]           Checksum mismatch

Check unavailable datanode error
    [arguments]                     ${json}     ${datanode}
    ${datanodes_b1} =               Create List   ${json}[blocks][0][replicas][0][hostname]    ${json}[blocks][0][replicas][1][hostname]   ${json}[blocks][0][replicas][2][hostname]
    ${index_b1} =                   Get Index From List     ${datanodes_b1}        ${datanode}
    Should Contain                  ${json}[blocks][0][replicas][${index_b1}][exception]           UNAVAILABLE
    ${datanodes_b2} =               Create List   ${json}[blocks][1][replicas][0][hostname]    ${json}[blocks][1][replicas][1][hostname]   ${json}[blocks][1][replicas][2][hostname]
    ${index_b2} =                   Get Index From List     ${datanodes_b2}        ${datanode}
    Should Contain                  ${json}[blocks][0][replicas][${index_b2}][exception]           UNAVAILABLE