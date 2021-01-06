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
Documentation       Ozone FS tests
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            setup.robot
Test Timeout        5 minutes
Suite Setup         Setup for FS test

*** Test Cases ***
List root
    ${root} =      Format FS URL         ${SCHEME}     ${VOLUME}    ${BUCKET}
                   Execute               ozone fs -ls ${root}

List non-existent volume
    ${url} =       Format FS URL         ${SCHEME}    no-such-volume    ${BUCKET}
    ${result} =    Execute and checkrc   ozone fs -ls ${url}     1
                   Should Match Regexp   ${result}         (Check access operation failed)|(Volume no-such-volume is not found)|(No such file or directory)

List non-existent bucket
    ${url} =       Format FS URL         ${SCHEME}    ${VOLUME}    no-such-bucket
    ${result} =    Execute and checkrc   ozone fs -ls ${url}     1
                   Should Match Regexp   ${result}         (Check access operation failed)|(Bucket not found)|(No such file or directory)

Create dir with parents
                   Execute               ozone fs -mkdir -p ${DEEP_URL}
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}         ${DEEP_DIR}

Copy from local
                   Execute               ozone fs -copyFromLocal NOTICE.txt ${DEEP_URL}/
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}         NOTICE.txt
    ${result} =    Execute               ozone sh key info ${VOLUME}/${BUCKET}/${DEEP_DIR}/NOTICE.txt | jq -r '.replicationFactor'
                   Should Be Equal       ${result}         3

Put
                   Execute               ozone fs -put NOTICE.txt ${DEEP_URL}/PUTFILE.txt
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}         PUTFILE.txt

List
    ${result} =    Execute               ozone fs -ls ${DEEP_URL}/
                   Should contain        ${result}         NOTICE.txt
                   Should contain        ${result}         PUTFILE.txt

Move
                   Execute               ozone fs -mv ${DEEP_URL}/NOTICE.txt ${DEEP_URL}/MOVED.TXT
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}         MOVED.TXT
                   Should not contain    ${result}       NOTICE.txt

Copy within FS
    [Setup]        Execute               ozone fs -mkdir -p ${DEEP_URL}/subdir1
                   Execute               ozone fs -cp ${DEEP_URL}/MOVED.TXT ${DEEP_URL}/subdir1/NOTICE.txt
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}         subdir1/NOTICE.txt

    ${result} =    Execute               ozone fs -ls ${DEEP_URL}/subdir1/
                   Should contain        ${result}         NOTICE.txt
                   Should not contain    ${result}       Failed

Cat file
                   Execute               ozone fs -cat ${DEEP_URL}/subdir1/NOTICE.txt

Delete file
                   Execute               ozone fs -rm -skipTrash ${DEEP_URL}/subdir1/NOTICE.txt
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should not contain    ${result}       NOTICE.txt

Delete dir
    ${result} =    Execute               ozone fs -rmdir ${DEEP_URL}/subdir1/
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should not contain    ${result}       subdir1

Touch file
                   Execute               ozone fs -touch ${DEEP_URL}/TOUCHFILE-${SCHEME}.txt
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should contain        ${result}       TOUCHFILE-${SCHEME}.txt

Delete file with Trash
                   Execute               ozone fs -touch ${DEEP_URL}/testFile.txt
                   Execute               ozone fs -rm ${DEEP_URL}/testFile.txt
    ${result} =    Execute               ozone fs -ls -R ${BASE_URL}/
                   Should not contain    ${result}     ${DEEP_URL}/testFile.txt
                   Should Contain Any    ${result}     .Trash/hadoop    .Trash/testuser/scm@EXAMPLE.COM    .Trash/root
                   Should contain        ${result}     ${DEEP_DIR}/testFile.txt

Delete recursively
                   Execute               ozone fs -mkdir -p ${DEEP_URL}/subdir2
                   Execute               ozone fs -rm -skipTrash -r ${DEEP_URL}/subdir2
    ${result} =    Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                   Should not contain    ${result}       ${DEEP_DIR}/subdir2

List recursively
    [Setup]        Setup localdir1
    ${result} =    Execute               ozone fs -ls -R ${BASE_URL}testdir1/
                   Should contain        ${result}         localdir1/LOCAL.txt
                   Should contain        ${result}         testdir1/NOTICE.txt

Copy to other bucket
    ${target} =    Format FS URL         ${SCHEME}    ${VOLUME}    ${BUCKET2}   testdir2
                   Execute               ozone fs -mkdir -p ${target}
                   Execute               ozone fs -cp ${BASE_URL}/testdir1/localdir1 ${target}
    [Teardown]     Execute               ozone fs -rm -r ${target}

Copy to other volume
    ${target} =    Format FS URL         ${SCHEME}    ${VOL2}    ${BUCKET_IN_VOL2}   testdir3
                   Execute               ozone fs -mkdir -p ${target}
                   Execute               ozone fs -cp ${BASE_URL}/testdir1/localdir1 ${target}
    [Teardown]     Execute               ozone fs -rm -r ${target}

List file created via shell
    [Setup]        Execute               ozone sh key put ${VOLUME}/${BUCKET}/${SCHEME}.txt NOTICE.txt
    ${result} =    Execute               ozone fs -ls ${BASE_URL}${SCHEME}.txt
                   Should contain        ${result}         ${SCHEME}.txt

Reject overwrite existing
    ${result} =    Execute and checkrc   ozone fs -copyFromLocal NOTICE.txt ${BASE_URL}${SCHEME}.txt    1
                   Should contain        ${result}         File exists

Get file
    [Setup]        Execute               rm -Rf /tmp/GET.txt
                   Execute               ozone fs -get ${BASE_URL}${SCHEME}.txt /tmp/GET.txt
                   File Should Exist     /tmp/GET.txt

*** Keywords ***

Setup localdir1
                   Execute               rm -Rf /tmp/localdir1
                   Execute               mkdir /tmp/localdir1
                   Execute               cp NOTICE.txt /tmp/localdir1/LOCAL.txt
                   Execute               ozone fs -mkdir -p ${BASE_URL}testdir1
                   Execute               ozone fs -copyFromLocal /tmp/localdir1 ${BASE_URL}testdir1/
                   Execute               ozone fs -put NOTICE.txt ${BASE_URL}testdir1/NOTICE.txt
