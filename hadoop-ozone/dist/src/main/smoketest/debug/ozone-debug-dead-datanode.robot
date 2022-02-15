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
Documentation       Test read-replicas in case of one datanode is unavailable
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ozone-debug.robot
Test Timeout        5 minute
*** Variables ***
${VOLUME}           cli-debug-volume
${BUCKET}           cli-debug-bucket
${TESTFILE}         testfile

*** Test Cases ***
Test ozone debug read-replicas with one datanode DEAD
    ${directory} =                      Execute read-replicas CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     5
    ${dn1_md5sum} =                     Execute     cat ${directory}/${TESTFILE}_block1_ozone_datanode_1.ozone_default ${directory}/${TESTFILE}_block2_ozone_datanode_1.ozone_default | md5sum | awk '{print $1}'
    ${dn3_md5sum} =                     Execute     cat ${directory}/${TESTFILE}_block1_ozone_datanode_3.ozone_default ${directory}/${TESTFILE}_block2_ozone_datanode_3.ozone_default | md5sum | awk '{print $1}'
    ${testfile_md5sum} =                Execute     md5sum testfile | awk '{print $1}'
    Should Be Equal                     ${dn1_md5sum}   ${testfile_md5sum}
    Should Be Equal                     ${dn3_md5sum}   ${testfile_md5sum}
    ${manifest} =                       Get File        ${directory}/${TESTFILE}_manifest
    ${json} =                           Evaluate        json.loads('''${manifest}''')        json
    Compare JSON                        ${json}
    ${datanodes_expected} =             Create List  ozone_datanode_1.ozone_default  ozone_datanode_3.ozone_default
    ${datanodes_b1} =                   Create List   ${json}[blocks][0][replicas][0][hostname]    ${json}[blocks][0][replicas][1][hostname]
    Check for datanodes                 ${datanodes_b1}    ${datanodes_expected}
    ${datanodes_b2} =                   Create List   ${json}[blocks][1][replicas][0][hostname]    ${json}[blocks][1][replicas][1][hostname]
    Check for datanodes                 ${datanodes_b2}    ${datanodes_expected}