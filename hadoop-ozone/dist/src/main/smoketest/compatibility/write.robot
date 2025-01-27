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
