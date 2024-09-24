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
Documentation       Test ozone debug ldb CLI
Library             OperatingSystem
Resource            ../lib/os.robot
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
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
    Execute             ozone sh volume create ${VOLUME}
    Execute             ozone sh bucket create ${VOLUME}/${BUCKET} -l OBJECT_STORE
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=100000 count=15
    Execute             ozone sh key put ${VOLUME}/${BUCKET}/${TESTFILE}1 ${TEMP_DIR}/${TESTFILE}
    Execute             ozone sh key put ${VOLUME}/${BUCKET}/${TESTFILE}2 ${TEMP_DIR}/${TESTFILE}
    Execute             ozone sh key put ${VOLUME}/${BUCKET}/${TESTFILE}3 ${TEMP_DIR}/${TESTFILE}
    Execute             ozone sh key addacl -a user:systest:a ${VOLUME}/${BUCKET}/${TESTFILE}3

*** Test Cases ***
Test ozone debug ldb ls
    ${output} =         Execute          ozone debug ldb --db=/data/metadata/om.db ls
                        Should contain      ${output}       keyTable

Test ozone debug ldb scan
    # test count option
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --count
                        Should Not Be Equal     ${output}       0
    # test valid json for scan command
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable | jq -r '.'
                        Should contain          ${output}       keyName
                        Should contain          ${output}       testfile1
                        Should contain          ${output}       testfile2
                        Should contain          ${output}       testfile3
    # test startkey option
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --startkey="/cli-debug-volume/cli-debug-bucket/testfile2"
                        Should not contain      ${output}       testfile1
                        Should contain          ${output}       testfile2
                        Should contain          ${output}       testfile3
    # test endkey option
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --endkey="/cli-debug-volume/cli-debug-bucket/testfile2"
                        Should contain          ${output}       testfile1
                        Should contain          ${output}       testfile2
                        Should not contain      ${output}       testfile3
    # test fields option
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --fields="volumeName,bucketName,keyName"
                        Should contain          ${output}       volumeName
                        Should contain          ${output}       bucketName
                        Should contain          ${output}       keyName
                        Should not contain      ${output}       objectID
                        Should not contain      ${output}       dataSize
                        Should not contain      ${output}       keyLocationVersions
    # test filter option with one filter
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --filter="keyName:equals:testfile2"
                        Should not contain      ${output}       testfile1
                        Should contain          ${output}       testfile2
                        Should not contain      ${output}       testfile3
    # test filter option with one multi-level filter
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --filter="acls.name:equals:systest"
                        Should not contain      ${output}       testfile1
                        Should not contain      ${output}       testfile2
                        Should contain          ${output}       testfile3
    # test filter option with multiple filter
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --filter="keyName:equals:testfile3,acls.name:equals:systest"
                        Should not contain      ${output}       testfile1
                        Should not contain      ${output}       testfile2
                        Should contain          ${output}       testfile3
    # test filter option with no records match both filters
    ${output} =         Execute                 ozone debug ldb --db=/data/metadata/om.db scan --cf=keyTable --filter="acls.name:equals:systest,keyName:equals:testfile2"
                        Should not contain      ${output}       testfile1
                        Should not contain      ${output}       testfile2
                        Should not contain      ${output}       testfile3
