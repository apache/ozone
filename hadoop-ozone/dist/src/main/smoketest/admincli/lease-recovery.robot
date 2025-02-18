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
Documentation       Test lease recovery of ozone filesystem
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ../lib/fs.robot
Test Timeout        5 minute
Suite Setup         Create volume bucket and put key

*** Variables ***
${OM_SERVICE_ID}    %{OM_SERVICE_ID}
${VOLUME}           lease-recovery-volume
${BUCKET}           lease-recovery-bucket
${TESTFILE}         testfile22

*** Keywords ***
Create volume bucket and put key
    Execute                 ozone sh volume create /${VOLUME}
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}
    Create File             ${TEMP_DIR}/${TESTFILE}
    Execute                 ozone sh key put /${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}

Execute Lease recovery cli
    [Arguments]             ${KEY_PATH}
    ${result} =             Execute And Ignore Error      ozone admin om lease recover --path=${KEY_PATH}
    [Return]                ${result}

*** Test Cases ***
Test ozone admin om lease recover for o3fs
    ${o3fs_path} =     Format FS URL         o3fs    ${VOLUME}    ${BUCKET}    ${TESTFILE}
    ${result} =        Execute Lease recovery cli    ${o3fs_path}
                       Should Contain    ${result}   Lease recovery SUCCEEDED
    ${o3fs_path} =     Format FS URL         o3fs    ${VOLUME}    ${BUCKET}    randomfile
    ${result} =        Execute Lease recovery cli    ${o3fs_path}
                       Should Contain    ${result}    not found

Test ozone admin om lease recover for ofs
    ${ofs_path} =      Format FS URL         ofs     ${VOLUME}    ${BUCKET}    ${TESTFILE}
    ${result} =        Execute Lease recovery cli    ${ofs_path}
                       Should Contain    ${result}   Lease recovery SUCCEEDED
    ${ofs_path} =      Format FS URL         ofs     ${VOLUME}    ${BUCKET}    randomfile
    ${result} =        Execute Lease recovery cli    ${ofs_path}
                       Should Contain    ${result}    not found
