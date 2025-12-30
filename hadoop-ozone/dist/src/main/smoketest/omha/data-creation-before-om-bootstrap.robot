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
Documentation       Smoke test for creating data needed for om bootstrap load test.
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Variables ***
${TMP_FILE}    ${TEMP_DIR}/tmp.txt
${VOLUME}
${BUCKET}
${SNAP_1}
${SNAP_2}
${KEY_PREFIX}
${KEY_1}
${KEY_2}

*** Keywords ***
Create volume and bucket
    [arguments]         ${volume}           ${bucket}
    ${vol_res} =        Execute             ozone sh volume create /${volume}
                        Should Be Empty     ${vol_res}
    ${bucket_res} =     Execute             ozone sh bucket create /${volume}/${bucket}
                        Should Be Empty     ${bucket_res}

Create a key and set contents same as the keyName
    [arguments]         ${volume}           ${bucket}      ${key_prefix}       ${key_name}         ${tmp_file}
    Execute             echo "${key_prefix}/${key_name}" > ${tmp_file}
    ${key_res} =        Execute             ozone sh key put /${volume}/${bucket}/${key_prefix}/${key_name} ${tmp_file}
                        Should Be Empty     ${key_res}
    ${key_cat_res} =    Execute             ozone sh key cat /${volume}/${bucket}/${key_prefix}/${key_name}
                        Should contain      ${key_cat_res}      ${key_prefix}/${key_name}

Create actual keys
    [arguments]         ${volume}           ${bucket}           ${key_prefix}       ${key_1}            ${key2}             ${tmp_file}
    Create a key and set contents same as the keyName           ${volume}           ${bucket}           ${key_prefix}       ${key_1}            ${tmp_file}
    Create a key and set contents same as the keyName           ${volume}           ${bucket}           ${key_prefix}       ${key_2}            ${tmp_file}

Create metadata keys
    [arguments]         ${threads}          ${key_num}          ${volume}       ${bucket}
    ${freon_res} =      Execute             ozone freon omkg -t ${threads} -n ${key_num} -v ${volume} -b ${bucket}
                        Should contain      ${freon_res}        Successful executions: ${key_num}

Create snapshot
    [arguments]         ${volume}           ${bucket}       ${snapshot}
    ${snap_res} =       Execute             ozone sh snapshot create /${volume}/${bucket} ${snapshot}
                        Should Be Empty     ${snap_res}

*** Test Cases ***
Volume-bucket init
    Create volume and bucket        ${VOLUME}       ${BUCKET}

Create 100 metadata keys under /${VOLUME}/${BUCKET}
    Create metadata keys     10      100             ${VOLUME}       ${BUCKET}

Create snapshot '${SNAP_1}'
    Create snapshot         ${VOLUME}               ${BUCKET}       ${SNAP_1}

Create 2 actual keys with prefix '${KEY_PREFIX}', key contents the same as the key name
    Create actual keys      ${VOLUME}               ${BUCKET}       ${KEY_PREFIX}       ${KEY_1}        ${KEY_2}        ${TMP_FILE}
    [teardown]    Remove File    ${TMP_FILE}

Create snapshot '${SNAP_2}'
    Create snapshot         ${VOLUME}               ${BUCKET}       ${SNAP_2}
