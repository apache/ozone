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
    Execute             ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME} --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} --space-quota 1TB
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=100000 count=15
    Execute             ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}

*** Test Cases ***
Test ozone debug replicas verify checksums
    ${output} =    Execute   ozone debug replicas verify --checksums --block-existence --container-state o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${TESTFILE}
    ${json} =      Evaluate  json.loads('''${output}''')      json

    # 'keys' array should be empty if all keys and their replicas passed checksum verification
    Should Be Empty      ${json}[keys]
    Should Be True       ${json}[pass]     ${True}

Test ozone debug version
    ${output} =    Execute    ozone debug version
                   Execute    echo '${output}' | jq -r '.' # validate JSON
