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
Documentation       Read Compatibility
Resource            ../ozone-lib/shell.robot
Resource            setup.robot
Test Timeout        5 minutes
Suite Setup         Create Local Test File

*** Variables ***
${SUFFIX}    ${EMPTY}

*** Test Cases ***
Bucket Replication Config
    Verify Bucket Empty Replication Config    /vol1/bucket1

Key Can Be Read
    Key Should Match Local File    /vol1/bucket1/key-${SUFFIX}    ${TESTFILE}

Encrypted Key Can Be Read
    Key Should Match Local File    /vol1/encrypted-${SUFFIX}/key    ${TESTFILE}

Dir Can Be Listed
    ${result} =     Execute    ozone fs -ls o3fs://bucket1.vol1/dir-${SUFFIX}
                    Should contain    ${result}    dir-${SUFFIX}/file-${SUFFIX}
    ${result} =     Execute    ozone fs -ls o3fs://bucket1.vol1/dir-${SUFFIX}/file-${SUFFIX}
                    Should contain    ${result}    dir-${SUFFIX}/file-${SUFFIX}

Dir Can Be Listed Using Shell
    Pass Execution If    '${DATA_VERSION}' >= '${EC_VERSION}'      New client creates RATIS/ONE key by default: BUG?

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        ${result} =     Key List With Replication    /vol1/bucket1
                        Should contain    ${result}    key-${SUFFIX} RATIS 3
    ELSE
        ${result} =     Execute    ozone sh key list /vol1/bucket1
                        Should Contain    ${result}    key-${SUFFIX}
    END

File Can Be Get
    Key Should Match Local File    /vol1/bucket1/dir-${SUFFIX}/file-${SUFFIX}    ${TESTFILE}

    Execute    ozone fs -get o3fs://bucket1.vol1/dir-${SUFFIX}/file-${SUFFIX} ${TEST_DATA_DIR}/
    Execute    diff -q ${TESTFILE} ${TEST_DATA_DIR}/file-${SUFFIX}
    [teardown]    Execute    rm -f ${TEST_DATA_DIR}/file-${SUFFIX}

FSO Bucket Can Be Read
    Pass Execution If    '${DATA_VERSION}' < '${FSO_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' < '${FSO_VERSION}'    Client does not support FSO
    Pass Execution If    '${CLUSTER_VERSION}' < '${FSO_VERSION}'   Cluster does not support FSO
    Execute    ozone fs -get ofs://om/vol1/fso-bucket-${SUFFIX}/dir/subdir/file ${TEMP_DIR}/
    Execute    diff -q ${TESTFILE} ${TEMP_DIR}/file
    [teardown]    Execute    rm -f ${TEMP_DIR}/file

HSync Lease Recover Can Be Used
    Pass Execution If    '${DATA_VERSION}' < '${FSO_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' < '${HSYNC_VERSION}'    Client does not support HSYNC
    Pass Execution If    '${CLUSTER_VERSION}' < '${HSYNC_VERSION}'   Cluster does not support HSYNC
    Execute    ozone debug recover --path=ofs://om/vol1/fso-bucket-${SUFFIX}/dir/subdir/file

Key Read From Bucket With Replication
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    Key Should Match Local File     /vol1/ratis-${SUFFIX}/3mb      ${TEST_DATA_DIR}/3mb

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        Assert Unsupported    ozone sh key get -f /vol1/ecbucket-${SUFFIX}/3mb /dev/null
    ELSE
        Key Should Match Local File     /vol1/ecbucket-${SUFFIX}/3mb      ${TEST_DATA_DIR}/3mb
    END

EC Test Listing Compat
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    ${result} =     Execute     ozone sh bucket list /vol1/ | jq -r '.name'
                    Should contain  ${result}   ratis
                    Should contain  ${result}   ec

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        ${result} =     Key List With Replication    /vol1/ratis-${SUFFIX}/
                        Should contain  ${result}   3mb RATIS 3

        Assert Unsupported    ozone sh key list /vol1/ecbucket-${SUFFIX}/
    END

EC Test Info Compat
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' >= '${EC_VERSION}'    Applies only to pre-EC client
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    ${result} =     Execute     ozone sh volume info vol1 | jq -r '.name'
                    Should contain  ${result}   vol1
    ${result} =     Bucket Replication    /vol1/ratis-${SUFFIX}
                    Should contain  ${result}   ratis        # there is no replication config in the old client for bucket info
    ${result} =     Bucket Replication    /vol1/ecbucket-${SUFFIX}
                    Should contain  ${result}   ec        # there is no replication config in the old client for bucket info

EC Test FS Compat
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' >= '${EC_VERSION}'    Applies only to pre-EC client
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    ${result} =     Execute     ozone fs -ls ofs://om/
                    Should contain  ${result}   /vol1
    ${result} =     Execute     ozone fs -ls ofs://om/vol1/
                    Should contain  ${result}   /vol1/ratis-${SUFFIX}
                    Should contain  ${result}   /vol1/ecbucket-${SUFFIX}
    ${result} =     Execute     ozone fs -ls ofs://om/vol1/ratis-${SUFFIX}/3mb
                    Should contain  ${result}   /vol1/ratis-${SUFFIX}/3mb

    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${SUFFIX}/     1
                    Should contain  ${result}   ls: The list of keys contains keys with Erasure Coded replication set
    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${SUFFIX}/3mb     1
                    Should contain  ${result}   : No such file or directory
    ${result} =     Execute and checkrc    ozone fs -get ofs://om/vol1/ecbucket-${SUFFIX}/3mb    1
                    Should contain  ${result}   : No such file or directory

EC Test FS Client Can Read Own Writes
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' >= '${EC_VERSION}'    Applies only to pre-EC client
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    Execute         ozone fs -put ${TEST_DATA_DIR}/1mb ofs://om/vol1/ratis-${SUFFIX}/1mb
    Execute         ozone fs -put ${TEST_DATA_DIR}/1mb ofs://om/vol1/ecbucket-${SUFFIX}/1mb
    Key Should Match Local File     /vol1/ratis-${SUFFIX}/1mb      ${TEST_DATA_DIR}/1mb
    Key Should Match Local File     /vol1/ecbucket-${SUFFIX}/1mb      ${TEST_DATA_DIR}/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/vol1/ratis-${SUFFIX}/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/vol1/ecbucket-${SUFFIX}/1mb

EC Test Client Can Read Own Writes
    Pass Execution If    '${DATA_VERSION}' < '${EC_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' >= '${EC_VERSION}'    Applies only to pre-EC client
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    Execute         ozone sh key put /vol1/ratis-${SUFFIX}/2mb ${TEST_DATA_DIR}/2mb
    Execute         ozone sh key put /vol1/ecbucket-${SUFFIX}/2mb ${TEST_DATA_DIR}/2mb
    Key Should Match Local File     /vol1/ratis-${SUFFIX}/2mb      ${TEST_DATA_DIR}/2mb
    Key Should Match Local File     /vol1/ecbucket-${SUFFIX}/2mb      ${TEST_DATA_DIR}/2mb
    Execute         ozone sh key delete /vol1/ratis-${SUFFIX}/2mb
    Execute         ozone sh key delete /vol1/ecbucket-${SUFFIX}/2mb
