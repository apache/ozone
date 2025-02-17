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
Documentation       Test ozone Debug CLI for EC(3,2) replicated keys
Library             OperatingSystem
Library             Process
Resource            ../lib/os.robot
Resource            ozone-debug.robot
Test Timeout        5 minute
Suite Setup         Create Volume Bucket

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-volume${PREFIX}
${BUCKET}           cli-debug-bucket
${TESTFILE}         testfile
${EC_DATA}          3
${EC_PARITY}        2

*** Keywords ***
Create Volume Bucket
    Execute             ozone sh volume create o3://om/${VOLUME}
    Execute             ozone sh bucket create o3://om/${VOLUME}/${BUCKET}

Create EC key
    [arguments]       ${bs}    ${count}    

    Execute           dd if=/dev/urandom of=${TEMP_DIR}/testfile bs=${bs} count=${count}
    Execute           ozone sh key put o3://om/${VOLUME}/${BUCKET}/testfile ${TEMP_DIR}/testfile -r rs-${EC_DATA}-${EC_PARITY}-1024k -t EC

*** Test Cases ***
0 data block
    Create EC key     1000    0
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     1

1 data block
    Create EC key     1048576    1
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     6
    ${sum_size} =                       Evaluate     1048576 * 3
    Verify Healthy EC Replica           ${directory}    1    ${sum_size}

2 data blocks
    Create EC key     1048576    2
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${sum_size} =                       Evaluate     1048576 * 4
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     6
    Verify Healthy EC Replica           ${directory}    1    ${sum_size}

3 data blocks
    Create EC key     1048576    3
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${sum_size} =                       Evaluate     1048576 * 5
    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     6
    Verify Healthy EC Replica           ${directory}    1    ${sum_size} 
    
3 data blocks and partial stripe
    Create EC key     1000000    4
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    ${sum_size} =                       Evaluate     1048576 * 5
    ${sum_size_last_stripe} =           Evaluate     ((1000000 * 4) % 1048576) * 3
    Should Be Equal As Integers         ${count_files}     11
    Verify Healthy EC Replica           ${directory}    1    ${sum_size}
    Verify Healthy EC Replica           ${directory}    2    ${sum_size_last_stripe}

4 data blocks and partial stripe
    Create EC key     1000000    5
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    ${sum_size} =                       Evaluate     1048576 * 5
    ${sum_size_last_stripe} =           Evaluate     1048576 * 3 + ((1000000 * 5) % 1048576)
    Should Be Equal As Integers         ${count_files}     11
    Verify Healthy EC Replica           ${directory}    1    ${sum_size}
    Verify Healthy EC Replica           ${directory}    2    ${sum_size_last_stripe}

6 data blocks
    Create EC key     1048576    6
    ${directory} =                      Execute replicas verify checksums CLI tool
    ${count_files} =                    Count Files In Directory    ${directory}
    ${sum_size} =                       Evaluate     1048576 * 5
    Should Be Equal As Integers         ${count_files}     11
    FOR    ${block}    IN RANGE    1    3
        Verify Healthy EC Replica       ${directory}    ${block}    ${sum_size}
    END
