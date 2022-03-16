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
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ozone-debug.robot
Test Timeout        5 minute
Suite Setup         Write keys
*** Variables ***
${VOLUME}           cli-debug-volume
${BUCKET}           cli-debug-bucket
${DEBUGKEY}         debugKey
${TESTFILE}         testfile

*** Keywords ***
Write keys
    Execute             ozone sh volume create o3://om/${VOLUME} --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create o3://om/${VOLUME}/${BUCKET}
    Execute             ozone sh key put o3://om/${VOLUME}/${BUCKET}/${DEBUGKEY} /opt/hadoop/NOTICE.txt
    Execute             dd if=/dev/urandom of=testfile bs=100000 count=15
    Execute             ozone sh key put o3://om/${VOLUME}/${BUCKET}/${TESTFILE} testfile

*** Test Cases ***
Test ozone debug chunkinfo
    ${result} =     Execute             ozone debug chunkinfo o3://om/${VOLUME}/${BUCKET}/${DEBUGKEY} | jq -r '.KeyLocations[0][0].Locations'
                    Should contain      ${result}       files
    ${result} =     Execute             ozone debug chunkinfo o3://om/${VOLUME}/${BUCKET}/${DEBUGKEY} | jq -r '.KeyLocations[0][0].Locations.files[0]'
                    File Should Exist   ${result}

Test ozone debug read-replicas
    ${directory} =                      Execute read-replicas CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     7
    ${dn1_md5sum} =                     Execute     cat ${directory}/${TESTFILE}_block1_ozone_datanode_1.ozone_default ${directory}/${TESTFILE}_block2_ozone_datanode_1.ozone_default | md5sum | awk '{print $1}'
    ${dn2_md5sum} =                     Execute     cat ${directory}/${TESTFILE}_block1_ozone_datanode_2.ozone_default ${directory}/${TESTFILE}_block2_ozone_datanode_2.ozone_default | md5sum | awk '{print $1}'
    ${dn3_md5sum} =                     Execute     cat ${directory}/${TESTFILE}_block1_ozone_datanode_3.ozone_default ${directory}/${TESTFILE}_block2_ozone_datanode_3.ozone_default | md5sum | awk '{print $1}'
    ${testfile_md5sum} =                Execute     md5sum testfile | awk '{print $1}'
    Should Be Equal                     ${dn1_md5sum}   ${testfile_md5sum}
    Should Be Equal                     ${dn2_md5sum}   ${testfile_md5sum}
    Should Be Equal                     ${dn3_md5sum}   ${testfile_md5sum}
    ${manifest} =                       Get File        ${directory}/${TESTFILE}_manifest
    ${json} =                           Evaluate        json.loads('''${manifest}''')        json
    Compare JSON                        ${json}
    Check for all datanodes             ${json}
