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
Documentation       Write Compatibility
Resource            ../ozone-lib/shell.robot
Resource            setup.robot
Resource            ../lib/fs.robot
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${ENCRYPTION_KEY}    key1


*** Test Cases ***
Create Bucket With Replication Type
    Pass Execution If    '${CLIENT_VERSION}' < '${EC_VERSION}'    Client does not support EC
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    Execute             ozone sh bucket create --replication 3 --type RATIS /vol1/ratis-${CLIENT_VERSION}
    Execute             ozone sh bucket create --replication rs-3-2-1024k --type EC /vol1/ecbucket-${CLIENT_VERSION}

Create Encrypted Bucket
    Execute    ozone sh bucket create -k ${ENCRYPTION_KEY} /vol1/encrypted-${CLIENT_VERSION}

Create Key in Encrypted Bucket
    Execute    ozone sh key put /vol1/encrypted-${CLIENT_VERSION}/key ${TESTFILE}

Key Can Be Written
    Create Key    /vol1/bucket1/key-${CLIENT_VERSION}    ${TESTFILE}

Key Can Be Written To Bucket With Replication Type
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    Execute         ozone sh key put /vol1/ratis-${CLUSTER_VERSION}/key-${CLIENT_VERSION} ${TESTFILE}
    Execute         ozone sh key put /vol1/ecbucket-${CLUSTER_VERSION}/key-${CLIENT_VERSION} ${TESTFILE}

Key Can Be Deleted
    Create Key    /vol1/bucket1/to-be-deleted-${CLIENT_VERSION}    ${TESTFILE}
    Execute       ozone sh key delete /vol1/bucket1/to-be-deleted-${CLIENT_VERSION}

Dir Can Be Created
    Execute    ozone fs -mkdir o3fs://bucket1.vol1/dir-${CLIENT_VERSION}

File Can Be Put
    Execute    ozone fs -put ${TESTFILE} o3fs://bucket1.vol1/dir-${CLIENT_VERSION}/file-${CLIENT_VERSION}

File Can Be Put To Bucket With Replication Type
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC
    Execute         ozone fs -put ${TESTFILE} ofs://om/vol1/ratis-${CLUSTER_VERSION}/file-${CLIENT_VERSION}
    Execute         ozone fs -put ${TESTFILE} ofs://om/vol1/ecbucket-${CLUSTER_VERSION}/file-${CLIENT_VERSION}

File Can Be Deleted
    Execute    ozone fs -put ${TESTFILE} o3fs://bucket1.vol1/dir-${CLIENT_VERSION}/to-be-deleted
    Execute    ozone fs -rm -skipTrash o3fs://bucket1.vol1/dir-${CLIENT_VERSION}/to-be-deleted

FSO Bucket Can Be Created and Used
    Pass Execution If    '${CLIENT_VERSION}' < '${FSO_VERSION}'    Client does not support FSO
    Pass Execution If    '${CLUSTER_VERSION}' < '${FSO_VERSION}'   Cluster does not support FSO
    Execute    ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /vol1/fso-bucket-${CLIENT_VERSION}
    Execute    ozone fs -mkdir -p ofs://om/vol1/fso-bucket-${CLIENT_VERSION}/dir/subdir
    Execute    ozone fs -put ${TESTFILE} ofs://om/vol1/fso-bucket-${CLIENT_VERSION}/dir/subdir/file

HSync Can Be Used To Create Keys
    Pass Execution If    '${CLIENT_VERSION}' < '${HSYNC_VERSION}'    Client does not support HSYNC
    Pass Execution If    '${CLUSTER_VERSION}' < '${HSYNC_VERSION}'   Cluster does not support HSYNC
    ${o3fspath} =   Format FS URL         o3fs     vol1    bucket1
    Freon DFSG      sync=HSYNC    n=1    path=${o3fspath}
    ${pfspath} =    Format FS URL         ofs      $vol1    bucket1
    Freon DFSG      sync=HSYNC    n=1    path=${pfspath}

Snapshot Diff RPC Is Compatible
    # Snapshot diff requires snapshot support in these tests.
    Pass Execution If    '${CLIENT_VERSION}' < '${SNAPSHOT_VERSION}'    Snapshot CLI not supported by this client
    Pass Execution If    '${CLUSTER_VERSION}' < '${SNAPSHOT_VERSION}'   Snapshot CLI not supported by this client

    ${bucket} =      Set Variable    snapdiff-${CLIENT_VERSION}
    ${fromSnap} =    Set Variable    snapdiff-from-${CLIENT_VERSION}
    ${toSnap} =      Set Variable    snapdiff-to-${CLIENT_VERSION}
    ${key1} =        Set Variable    snapdiff-key1-${CLIENT_VERSION}
    ${key2} =        Set Variable    snapdiff-key2-${CLIENT_VERSION}

    Execute          ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /vol1/${bucket}
    Execute          ozone sh key put /vol1/${bucket}/base ${TESTFILE}
    Execute          ozone sh snapshot create /vol1/${bucket} ${fromSnap}
    Execute          ozone sh key put /vol1/${bucket}/${key1} ${TESTFILE}
    Execute          ozone sh key put /vol1/${bucket}/${key2} ${TESTFILE}
    Execute          ozone sh snapshot create /vol1/${bucket} ${toSnap}

    Snapshot Diff Report Should Contain Created Keys
    ...    /vol1/${bucket}    ${fromSnap}    ${toSnap}    ${key1}    ${key2}

*** Keywords ***
Snapshot Diff Report Should Contain Created Keys
    [Arguments]    ${bucketPath}    ${fromSnap}    ${toSnap}    ${key1}    ${key2}

    # New clients support --get-report. Old clients don't; fall back to the legacy call.
    ${status}    ${output} =    Run Keyword And Ignore Error
    ...    Execute    ozone sh snapshot diff --get-report ${bucketPath} ${fromSnap} ${toSnap}

    IF    '${status}' == 'PASS'
        # On newer servers, --get-report does not submit jobs. If the job is not
        # found, submit it first (this keeps new-client/new-server behavior).
        ${notFound} =    Run Keyword And Return Status
        ...    Should Contain    ${output}    No snapshot diff job found
        IF    ${notFound}
            ${output} =    Execute    ozone sh snapshot diff ${bucketPath} ${fromSnap} ${toSnap}
                           Should Contain    ${output}    Submitting a new job
        ELSE
        # On older servers, --get-report falls back to the legacy snapshot diff command.
        # Ensure the submit path succeeds without failing the test.
            ${submitStatus}    ${submitOutput} =    Run Keyword And Ignore Error
            ...    Execute    ozone sh snapshot diff ${bucketPath} ${fromSnap} ${toSnap}
            Run Keyword If    '${submitStatus}' == 'FAIL'
            ...    Fail    Snapshot diff submit failed: ${submitOutput}
        END
        Wait Until Keyword Succeeds    2min    5sec
        ...    Snapshot Diff Command Should Contain Created Keys
        ...    ozone sh snapshot diff --get-report ${bucketPath} ${fromSnap} ${toSnap}
        ...    ${fromSnap}    ${toSnap}    ${key1}    ${key2}
    ELSE
        # old client, which does not support --get-report.
        # The snapshot diff command should return the report, submitting a job if necessary.
        Wait Until Keyword Succeeds    2min    5sec
        ...    Snapshot Diff Command Should Contain Created Keys
        ...    ozone sh snapshot diff ${bucketPath} ${fromSnap} ${toSnap}
        ...    ${fromSnap}    ${toSnap}    ${key1}    ${key2}
    END

Snapshot Diff Command Should Contain Created Keys
    [Arguments]    ${command}    ${fromSnap}    ${toSnap}    ${key1}    ${key2}
    ${output} =    Execute    ${command}
    Should Contain    ${output}    Difference between snapshot: ${fromSnap} and snapshot: ${toSnap}
    Should Contain    ${output}    ${key1}
    Should Contain    ${output}    ${key2}
