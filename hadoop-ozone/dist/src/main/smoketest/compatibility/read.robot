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


*** Keywords ***
Key List With Replication
    [arguments]    ${args}
    ${list} =      Execute    ozone sh key list ${args}
    ${result} =    Execute    echo '${list}' | jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'
    [return]    ${result}


*** Test Cases ***
Buckets Can Be Listed
    ${result} =     Execute     ozone sh bucket list /vol1
    Should Contain    ${result}    bucket1

    IF    '${CLUSTER_VERSION}' >= '${EC_VERSION}'
        Should Contain    ${result}    ratis-${CLUSTER_VERSION}
        Should Contain    ${result}    ecbucket-${CLUSTER_VERSION}
    END

Bucket Without Replication Config
    Verify Bucket Empty Replication Config    /vol1/bucket1

Bucket With Replication Config
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}'
        Verify Bucket Replica Replication Config    /vol1/ratis-${CLUSTER_VERSION}   RATIS   THREE
        Verify Bucket EC Replication Config    /vol1/ecbucket-${CLUSTER_VERSION}    RS    3    2    1048576
    ELSE
        Verify Bucket Empty Replication Config    /vol1/ratis-${CLUSTER_VERSION}
        Verify Bucket Empty Replication Config    /vol1/ecbucket-${CLUSTER_VERSION}
    END

Key Can Be Read
    Key Should Match Local File    /vol1/bucket1/key-${DATA_VERSION}    ${TESTFILE}

Encrypted Key Can Be Read
    Key Should Match Local File    /vol1/encrypted-${DATA_VERSION}/key    ${TESTFILE}
    File Should Match Local File    ofs://om/vol1/encrypted-${DATA_VERSION}/key    ${TESTFILE}

Key Read From Bucket With Replication
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    Key Should Match Local File     /vol1/ratis-${CLUSTER_VERSION}/key-${DATA_VERSION}      ${TESTFILE}

    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}' or '${DATA_VERSION}' == '${CLIENT_VERSION}'
        Key Should Match Local File     /vol1/ecbucket-${CLUSTER_VERSION}/key-${DATA_VERSION}      ${TESTFILE}
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
    IF    '${CLIENT_VERSION}' >= '${EC_VERSION}'
        ${result} =     Execute    ozone sh key list /vol1/bucket1
                        Should Contain    ${result}    key-${DATA_VERSION}
    ELSE IF    '${DATA_VERSION}' < '${EC_VERSION}' # New client creates RATIS/ONE key by default: BUG?
        ${result} =     Key List With Replication    /vol1/bucket1
                        Should contain    ${result}    key-${DATA_VERSION} RATIS 3
    END

Key List In Bucket With Replication
    Pass Execution If    '${CLUSTER_VERSION}' < '${EC_VERSION}'   Cluster does not support EC

    IF    '${CLIENT_VERSION}' < '${EC_VERSION}'
        ${result} =     Key List With Replication    /vol1/ratis-${CLUSTER_VERSION}/
                        Should contain  ${result}   key-${DATA_VERSION} RATIS 3

        Assert Unsupported    ozone sh key list /vol1/ecbucket-${CLUSTER_VERSION}/
    ELSE
        ${result} =     Execute    ozone sh key list /vol1/ratis-${CLUSTER_VERSION}
                        Should Contain    ${result}    key-${DATA_VERSION}
        ${result} =     Execute    ozone sh key list /vol1/ecbucket-${CLUSTER_VERSION}
                        Should Contain    ${result}    key-${DATA_VERSION}
    END


File Can Be Get
    Key Should Match Local File    /vol1/bucket1/dir-${DATA_VERSION}/file-${DATA_VERSION}    ${TESTFILE}
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
    File Should Match Local File    ofs://om/vol1/fso-bucket-${DATA_VERSION}/dir/subdir/file    ${TESTFILE}

HSync Lease Recover Can Be Used
    Pass Execution If    '${DATA_VERSION}' < '${FSO_VERSION}'      Skipped write test case
    Pass Execution If    '${CLIENT_VERSION}' < '${HSYNC_VERSION}'    Client does not support HSYNC
    Pass Execution If    '${CLUSTER_VERSION}' < '${HSYNC_VERSION}'   Cluster does not support HSYNC
    Execute    ozone admin om lease recover --path=ofs://om/vol1/fso-bucket-${DATA_VERSION}/dir/subdir/file

Key Info File Flag Should Be Set Correctly
    Pass Execution If    '${CLUSTER_VERSION}' <= '${EC_VERSION}'   Cluster does not support 'file' flag
    Pass Execution If    '${CLIENT_VERSION}' <= '${EC_VERSION}'    Client does not support 'file' flag

    ${dirpath} =      Set Variable    /vol1/fso-bucket-${DATA_VERSION}/dir/subdir/
    ${filepath} =     Set Variable    ${dirpath}file

    ${key_info} =     Execute    ozone sh key info ${filepath}
    Should Contain    ${key_info}    \"file\" : true

    ${dir_info} =     Execute    ozone sh key info ${dirpath}
    Should Contain    ${dir_info}    \"file\" : false

Container Balancer Status Command Runs
    # Container balancer command was introduced in version 1.2.0
    # Skip test if either client or cluster doesn't support it
    Pass Execution If    '${CLIENT_VERSION}' < '${CONTAINERBALANCER_VERSION}'    Client does not support container balancer
    Pass Execution If    '${CLUSTER_VERSION}' < '${CONTAINERBALANCER_VERSION}'   Cluster does not support container balancer

    ${result} =     Execute     ozone admin containerbalancer status
    Should Contain    ${result}    ContainerBalancer
