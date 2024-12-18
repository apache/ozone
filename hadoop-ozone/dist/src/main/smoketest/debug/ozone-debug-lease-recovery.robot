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
Resource            ozone-debug.robot
Test Timeout        5 minute
Suite Setup         Create volume bucket and put key

*** Variables ***
${VOLUME}           lease-recovery-volume
${BUCKET}           lease-recovery-bucket
${TESTFILE}         testfile22

*** Keywords ***
Create volume bucket and put key
    Execute                 ozone sh volume create /${VOLUME}
    Execute                 ozone sh bucket create /${VOLUME}/${BUCKET}
    Create File             ${TEMP_DIR}/${TESTFILE}
    Execute                 ozone sh key put /${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}

*** Test Cases ***
Test ozone debug recover for o3fs
    ${result} =              Execute Lease recovery cli    o3fs://${BUCKET}.${VOLUME}.om/${TESTFILE}
    Should Contain    ${result}   Lease recovery SUCCEEDED
    ${result} =              Execute Lease recovery cli    o3fs://${BUCKET}.${VOLUME}.om/randomfile
    Should Contain    ${result}    not found

Test ozone debug recover for ofs
    ${result} =              Execute Lease recovery cli    ofs://om/${VOLUME}/${BUCKET}/${TESTFILE}
    Should Contain    ${result}   Lease recovery SUCCEEDED
    ${result} =              Execute Lease recovery cli    ofs://om/${VOLUME}/${BUCKET}/randomfile
    Should Contain    ${result}    not found
