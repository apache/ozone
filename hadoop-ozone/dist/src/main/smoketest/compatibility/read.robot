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
Resource            ../snapshot/lib.resource
Resource            setup.robot
Resource            ../ozone-lib/batch.robot
Resource            ../ozone-lib/batch-shell.robot
Suite Setup         Setup Suite
Suite Teardown      Teardown Suite
Test Timeout        5 minutes


*** Variables ***
${FSO_BUCKET}        vol1/fso-bucket-${DATA_VERSION}
${SNAPSHOT1}         snapshot-1-${DATA_VERSION}
${SNAPSHOT2}         snapshot-2-${DATA_VERSION}
${SNAPSHOT3}         snapshot-3-${DATA_VERSION}
${SNAPSHOT_KEY1}     snapdiff-key1-${DATA_VERSION}
${SNAPSHOT_KEY2}     snapdiff-key2-${DATA_VERSION}
${KEY_REPLICATION_FILTER}    jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'


*** Test Cases ***
Buckets Can Be Listed
    ${result} =     Execute Later ozone sh    bucket list /vol1
    Assert Later    File ${result} Should    Contain    bucket1

    IF    '${CLUSTER_VERSION}' >= '${EC_VERSION}'
        Assert Later    File ${result} Should
        ...    Contain    ratis-${CLUSTER_VERSION}
        ...    Contain    ecbucket-${CLUSTER_VERSION}
    END

Bucket Without Replication Config
    ${result} =     Execute Later ozone sh    bucket info /vol1/bucket1
    Assert Later    Verify Empty Replication Config in ${result}

Bucket With Replication Config
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    ${ratis_result} =    Execute Later ozone sh    bucket info /vol1/ratis-${CLUSTER_VERSION}
    ${ec_result} =    Execute Later ozone sh    bucket info /vol1/ecbucket-${CLUSTER_VERSION}

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}'
        Assert Later    Verify Replica Replication Config in ${ratis_result}    RATIS   THREE
        Assert Later    Verify EC Replication Config in ${ec_result}    RS    3    2    1048576
    ELSE
        Assert Later    Verify Empty Replication Config in ${ratis_result}
        Assert Later    Verify Empty Replication Config in ${ec_result}
    END

Key Can Be Read
    ${result} =    Get Key Later    /vol1/bucket1/key-${DATA_VERSION}
    Assert Later    Compare Files    ${result}    ${TESTFILE}

Encrypted Key Can Be Read
    ${result} =    Get Key Later    /vol1/encrypted-${DATA_VERSION}/key
    Assert Later    Compare Files    ${result}    ${TESTFILE}

Encrypted File Can Be Read
    File Should Match Local File    ofs://om/vol1/encrypted-${DATA_VERSION}/key    ${TESTFILE}

Key Read From Bucket With Replication
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    ${get_from_ratis_bucket} =    Get Key Later    /vol1/ratis-${CLUSTER_VERSION}/key-${DATA_VERSION}
    Assert Later    Compare Files    ${get_from_ratis_bucket}    ${TESTFILE}

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}' or '${DATA_VERSION}' == '${CLIENT_VERSION}'
        ${ec_local} =    Get Next Filename    ${BATCH_OUTPUT_DIR}/key-get
        ${ec_result} =    Execute Later ozone sh    key get /vol1/ecbucket-${CLUSTER_VERSION}/key-${DATA_VERSION} ${ec_local}
        Assert Later    Compare Files    ${ec_local}    ${TESTFILE}
    ELSE
        Assert Unsupported    ozone sh key get -f /vol1/ecbucket-${CLUSTER_VERSION}/key-${DATA_VERSION} /dev/null
    END

Dir Can Be Listed
    ${result} =     Execute    ozone fs -ls o3fs://bucket1.vol1/dir-${DATA_VERSION}
                    Should contain    ${result}    dir-${DATA_VERSION}/file-${DATA_VERSION}

    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

#    TODO HDDS-11803
#    ${result} =     Execute     ozone fs -ls ofs://om/vol1/
#                    Should contain  ${result}   /vol1/ratis-${CLUSTER_VERSION}
#                    Should contain  ${result}   /vol1/ecbucket-${CLUSTER_VERSION}

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/     1
                        Should contain  ${result}   ls: The list of keys contains keys with Erasure Coded replication set
    END

File Can Be Listed
    ${result} =     Execute    ozone fs -ls o3fs://bucket1.vol1/dir-${DATA_VERSION}/file-${DATA_VERSION}
                    Should contain    ${result}    dir-${DATA_VERSION}/file-${DATA_VERSION}

    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    ${result} =     Execute     ozone fs -ls ofs://om/vol1/ratis-${CLUSTER_VERSION}/file-${DATA_VERSION}
                    Should contain  ${result}   /vol1/ratis-${CLUSTER_VERSION}/file-${DATA_VERSION}

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}' or '${DATA_VERSION}' == '${CLIENT_VERSION}'
        ${result} =     Execute    ozone fs -ls ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/file-${DATA_VERSION}
                        Should contain  ${result}   /vol1/ecbucket-${CLUSTER_VERSION}/file-${DATA_VERSION}
    ELSE
        ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/file-${DATA_VERSION}     1
                        Should contain  ${result}   : No such file or directory
    END

Key List
    ${result} =    Execute Later ozone sh    key list /vol1/bucket1

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}'
        Assert Later    File ${result} Should    Contain    key-${DATA_VERSION}
    ELSE IF    '${DATA_VERSION}' < '${EC_VERSION}' # New client creates RATIS/ONE key by default: BUG?
        Assert Later    File ${result} Filtered By ${KEY_REPLICATION_FILTER} Should    Contain    key-${DATA_VERSION} RATIS 3
    END

Key List In Bucket With Replication
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    ${ratis_result} =     Execute Later ozone sh    key list /vol1/ratis-${CLUSTER_VERSION}

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        Assert Later    File ${ratis_result} Filtered By ${KEY_REPLICATION_FILTER} Should    Contain    key-${DATA_VERSION} RATIS 3
        Assert Unsupported    ozone sh key list /vol1/ecbucket-${CLUSTER_VERSION}/
    ELSE
        Assert Later    File ${ratis_result} Should    Contain    key-${DATA_VERSION}
        ${ec_result} =     Execute Later ozone sh    key list /vol1/ecbucket-${CLUSTER_VERSION}
        Assert Later    File ${ec_result} Should    Contain    key-${DATA_VERSION}
    END

File Can Be Get
    ${file} =    Get Key Later    /vol1/bucket1/dir-${DATA_VERSION}/file-${DATA_VERSION}
    Assert Later    Compare Files    ${file}    ${TESTFILE}

File Can Be Get via FS
    File Should Match Local File    o3fs://bucket1.vol1/dir-${DATA_VERSION}/file-${DATA_VERSION}    ${TESTFILE}

File Can Be Get From Bucket With Replication
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    File Should Match Local File     ofs://om/vol1/ratis-${CLUSTER_VERSION}/file-${DATA_VERSION}      ${TESTFILE}

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}' or '${DATA_VERSION}' == '${CLIENT_VERSION}'
        File Should Match Local File     ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/key-${DATA_VERSION}      ${TESTFILE}
    ELSE
        ${result} =     Execute and checkrc    ozone fs -get ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/key-${DATA_VERSION}    1
                        Should contain  ${result}   : No such file or directory
    END

FSO Bucket Can Be Read
    Pass Execution If    '${DATA_VERSION}' < '${FSO_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' < '${FSO_VERSION}'    Client does not support FSO
    Pass Execution If    '${CLUSTER_VERSION}' < '${FSO_VERSION}'   Cluster does not support FSO
    File Should Match Local File    ofs://om/${FSO_BUCKET}/dir/subdir/file    ${TESTFILE}

HSync Lease Recover Can Be Used
    Pass Execution If    '${DATA_VERSION}' < '${FSO_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' < '${HSYNC_VERSION}'    Client does not support HSYNC
    Pass Execution If    '${CLUSTER_VERSION}' < '${HSYNC_VERSION}'   Cluster does not support HSYNC
    Execute    ozone admin om lease recover --path=ofs://om/${FSO_BUCKET}/dir/subdir/file

Key Info File Flag Should Be Set Correctly
    Pass Execution If    '${CLUSTER_VERSION}' <= '${EC_VERSION}'   Cluster does not support 'file' flag
    Pass Execution If    '${CLIENT_VERSION}' <= '${EC_VERSION}'    Client does not support 'file' flag

    ${dirpath} =      Set Variable    /${FSO_BUCKET}/dir/subdir/
    ${filepath} =     Set Variable    ${dirpath}file

    ${key_info} =     Execute Later ozone sh    key info ${filepath}
    Assert Later    File ${key_info} Should    Contain    \"file\" : true

    ${dir_info} =     Execute Later ozone sh    key info ${dirpath}
    Assert Later    File ${dir_info} Should    Contain    \"file\" : false

Snapshots Can Be Diffed
    Pass Execution If    '${CLUSTER_VERSION}' < '${SNAPSHOT_VERSION}'    Cluster does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' < '${SNAPSHOT_VERSION}'    Client does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' >= '${SNAPSHOT_DIFF_SUBMIT_VERSION}'    Tested elsewhere with --get-report
    Pass Execution If    '${DATA_VERSION}' < '${SNAPSHOT_VERSION}'    Skipped write test case

    # old client does not support --get-report
    # The snapshot diff command should return the report, submitting a job if necessary.
    Wait For Snapshot diff  ${FSO_BUCKET}  ${SNAPSHOT1}  ${SNAPSHOT2}  ${SNAPSHOT_KEY1}

Legacy Client Can Read --ordered Snapshot Diff
    Pass Execution If    '${CLUSTER_VERSION}' < '${SNAPSHOT_VERSION}'    Cluster does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' < '${SNAPSHOT_VERSION}'    Client does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' >= '${SNAPSHOT_DIFF_SUBMIT_VERSION}'    Tested elsewhere with --get-report
    Pass Execution If    '${DATA_VERSION}' < '${SNAPSHOT_DIFF_ORDERED_VERSION}'    Skipped write test case

    Wait For Snapshot diff  ${FSO_BUCKET}  ${SNAPSHOT1}  ${SNAPSHOT3}  ${SNAPSHOT_KEY1}  ${SNAPSHOT_KEY2}

Can --get-report of Snapshot Diff
    Pass Execution If    '${CLUSTER_VERSION}' < '${SNAPSHOT_VERSION}'    Cluster does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' < '${SNAPSHOT_DIFF_SUBMIT_VERSION}'    Client does not support snapshot diff --get-report
    Pass Execution If    '${DATA_VERSION}' < '${SNAPSHOT_VERSION}'    Skipped write test case

    Wait For Snapshot diff --get-report  ${FSO_BUCKET}  ${SNAPSHOT1}  ${SNAPSHOT2}  ${SNAPSHOT_KEY1}

Can --get-report of --ordered Snapshot Diff
    Pass Execution If    '${CLUSTER_VERSION}' < '${SNAPSHOT_VERSION}'    Cluster does not support snapshots
    Pass Execution If    '${CLIENT_VERSION}' < '${SNAPSHOT_DIFF_SUBMIT_VERSION}'    Client does not support snapshot diff --get-report
    Pass Execution If    '${DATA_VERSION}' < '${SNAPSHOT_DIFF_ORDERED_VERSION}'    Skipped write test case

    ${output} =    Wait For Snapshot diff --get-report  ${FSO_BUCKET}  ${SNAPSHOT1}  ${SNAPSHOT3}  ${SNAPSHOT_KEY1}  ${SNAPSHOT_KEY2}
    IF    '${CLUSTER_VERSION}' >= '${SNAPSHOT_DIFF_ORDERED_VERSION}' and '${CLIENT_VERSION}' >= '${SNAPSHOT_DIFF_ORDERED_VERSION}'
        Should Contain    ${output}    Report is dependency ordered
    END

Execute Batch Shell
    Execute Batch
