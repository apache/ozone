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
Documentation       Test read-replicas in case of a corrupt replica
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ozone-debug.robot
Test Timeout        5 minute
*** Variables ***
${PREFIX}              ${EMPTY}
${VOLUME}              cli-debug-volume${PREFIX}
${BUCKET}              cli-debug-bucket
${TESTFILE}            testfile
${CORRUPT_DATANODE}    ozone_datanode_1.ozone_default

*** Test Cases ***
Test ozone debug read-replicas with corrupt block replica
    ${directory} =                      Execute read-replicas CLI tool
    Set Test Variable    ${DIR}         ${directory}

    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     7

    ${json} =                           Read Replicas Manifest
    ${md5sum} =                         Execute     md5sum testfile | awk '{print $1}'

    FOR    ${replica}    IN RANGE    3
        ${datanode} =    Set Variable    ${json}[blocks][0][replicas][${replica}][hostname]

        IF    '${datanode}' == '${CORRUPT_DATANODE}'
            Verify Corrupt Replica   ${json}    ${replica}    ${md5sum}
            Should Contain           ${json}[blocks][0][replicas][${replica}][exception]    Checksum mismatch
        ELSE
            Verify Healthy Replica   ${json}    ${replica}    ${md5sum}
        END
    END
