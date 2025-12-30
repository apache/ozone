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
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute
Suite Setup         Create Volume Bucket

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-ec-volume${PREFIX}
${BUCKET}           cli-debug-ec-bucket
${TESTFILE}         testfile
${EC_DATA}          3
${EC_PARITY}        2
${OM_SERVICE_ID}    %{OM_SERVICE_ID}

*** Keywords ***
Create Volume Bucket
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Execute             ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}

Create EC key
    [arguments]       ${bs}    ${count}    

    Execute           dd if=/dev/urandom of=${TEMP_DIR}/testfile bs=${bs} count=${count}
    Execute           ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/testfile ${TEMP_DIR}/testfile -r rs-${EC_DATA}-${EC_PARITY}-1024k -t EC

*** Test Cases ***
Test ozone debug replicas chunk-info
    Create EC key     1048576    3
    ${count} =        Execute           ozone debug replicas chunk-info o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/testfile | jq '[.keyLocations[0][] | select(.file | test("\\\\.block$")) | .file] | length'
    Should Be Equal As Integers         ${count}          5
