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
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-volume${PREFIX}
${BUCKET}           cli-debug-bucket
${DEBUGKEY}         debugKey
${TESTFILE}         testfile

*** Keywords ***
Write keys
    Execute             ozone sh volume create o3://om/${VOLUME} --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create o3://om/${VOLUME}/${BUCKET} --space-quota 1TB
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=100000 count=15
    Execute             ozone sh key put o3://om/${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}

*** Test Cases ***
Test ozone debug read-replicas
    ${directory} =                      Execute replicas verify checksums CLI tool
    Set Test Variable    ${DIR}         ${directory}

    ${count_files} =                    Count Files In Directory    ${directory}
    Should Be Equal As Integers         ${count_files}     7

    ${json} =                           Read Replicas Manifest
    ${md5sum} =                         Execute     md5sum ${TEMP_DIR}/${TESTFILE} | awk '{print $1}'

    FOR    ${replica}    IN RANGE    3
        Verify Healthy Replica   ${json}    ${replica}    ${md5sum}
    END


Test ozone debug version
    ${output} =    Execute    ozone debug version
                   Execute    echo '${output}' | jq -r '.' # validate JSON
