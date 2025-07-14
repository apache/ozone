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
Documentation       Test that FS operations fail on OBS buckets
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ../lib/fs.robot
Test Timeout        5 minutes

*** Variables ***
${SCHEME}           ofs
${volume}           obs-volume1
${bucket}           obs-bucket1
${PREFIX}           ozone

*** Test Cases ***

Create OBS bucket
    Execute             ozone sh volume create /${volume}
    Execute             ozone sh bucket create /${volume}/${bucket} --layout OBJECT_STORE

Verify mkdir fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testdir
    ${result} =         Execute and checkrc   ozone fs -mkdir ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify put fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -put NOTICE.txt ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify ls fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}
    ${result} =         Execute and checkrc   ozone fs -ls ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Create key in OBS bucket
    Execute             ozone sh key put /${volume}/${bucket}/testfile NOTICE.txt

Verify ls fails on OBS bucket key
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -ls ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify rm fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -rm ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify mv fails on OBS bucket
    ${url1} =           Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${url2} =           Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile2
    ${result} =         Execute and checkrc   ozone fs -mv ${url1} ${url2}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify cp fails on OBS bucket
    ${url1} =           Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${url2} =           Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile2
    ${result} =         Execute and checkrc   ozone fs -cp ${url1} ${url2}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify touch fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -touch ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify cat fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -cat ${url}     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.

Verify get fails on OBS bucket
    ${url} =            Format FS URL         ${SCHEME}    ${volume}    ${bucket}    testfile
    ${result} =         Execute and checkrc   ozone fs -get ${url} /tmp/testfilecopy     255
    Should contain      ${result}             Bucket: ${bucket} has layout: OBJECT_STORE, which does not support file system semantics. Bucket Layout must be FILE_SYSTEM_OPTIMIZED or LEGACY.
