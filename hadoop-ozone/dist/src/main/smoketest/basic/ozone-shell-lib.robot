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
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Resource            ../ozone-lib/batch.robot
Resource            ../ozone-lib/batch-shell.robot

*** Variables ***
${prefix}    generated
${SCM}       scm

*** Keywords ***

Generate prefix
   ${random} =         Generate Random String  5  [NUMBERS]
   Set Suite Variable  ${prefix}  ${random}

Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}

    ${result} =     Execute Later ozone sh    volume create ${protocol}${server}/${volume} --space-quota 100TB --namespace-quota 100
    Assert Later    File Should Be Empty     ${result}

    ${result} =     Execute Later ozone sh    volume list ${protocol}${server}/
    ${filter} =     Set Variable    jq -r '.[] | select(.name=="${volume}")'
    Assert Later    File ${result} Filtered By ${filter} Should    Contain    creationTime

# TODO: Disable updating the owner, acls should be used to give access to other user.
    Execute Later ozone sh    volume setquota ${protocol}${server}/${volume} --space-quota 10TB --namespace-quota 100
#    ${result} =     Execute Later ozone sh    volume info ${protocol}${server}/${volume}
#    ${filter} =     Set Variable    jq -r '. | select(.volumeName=="${volume}") | .owner | .name'
#                    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal     bill

    ${result} =     Execute Later ozone sh    volume info ${protocol}${server}/${volume}
    ${filter} =     Set Variable    jq -r '. | select(.name=="${volume}") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       10995116277760

    ${result} =     Execute Later ozone sh    bucket create ${protocol}${server}/${volume}/bb1 --space-quota 10TB --namespace-quota 100
    Assert Later    File Should Be Empty     ${result}

    ${result} =     Execute Later ozone sh    bucket info ${protocol}${server}/${volume}/bb1
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .storageType'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       DISK
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       10995116277760
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       100

    Execute Later ozone sh    bucket setquota ${protocol}${server}/${volume}/bb1 --space-quota 1TB --namespace-quota 1000
    ${result} =     Execute Later ozone sh    bucket info ${protocol}${server}/${volume}/bb1
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       1099511627776
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       1000

    ${result} =     Execute Later ozone sh    bucket list ${protocol}${server}/${volume}/
    ${filter} =     Set Variable    jq -r '.[] | select(.name=="bb1") | .volumeName'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       ${volume}

    Test key handling    ${protocol}    ${server}    ${volume}

    Execute Later ozone sh    volume clrquota --space-quota ${protocol}${server}/${volume}
    ${result} =     Execute Later ozone sh    volume info ${protocol}${server}/${volume}
    ${filter} =     Set Variable    jq -r '. | select(.name=="${volume}") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    volume clrquota --namespace-quota ${protocol}${server}/${volume}
    ${result} =     Execute Later ozone sh    volume info ${protocol}${server}/${volume}
    ${filter} =     Set Variable    jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    bucket clrquota --space-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute Later ozone sh    bucket info ${protocol}${server}/${volume}/bb1
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    bucket clrquota --namespace-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute Later ozone sh    bucket info ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '. | select(.name=="bb1") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    bucket delete -r --yes ${protocol}${server}/${volume}/bb1
    Execute Later ozone sh    volume delete ${protocol}${server}/${volume}
    Execute Later ozone sh    volume create ${protocol}${server}/${volume}
    ${result} =     Execute Later ozone sh    volume info ${protocol}${server}/${volume}
    ${filter} =     Set Variable    jq -r '. | select(.name=="${volume}") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1
    ${filter} =     Set Variable    jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute Later ozone sh    bucket info ${protocol}${server}/${volume}/bb1
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInBytes'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1
    ${filter} =     Set Variable    jq -r '. | select(.name=="bb1") | .quotaInNamespace'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       -1

    Execute Later ozone sh    bucket delete ${protocol}${server}/${volume}/bb1
    Execute Later ozone sh    volume delete ${protocol}${server}/${volume}

Test ozone shell errors
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute and checkrc    ozone sh volume info ${protocol}${server}/${volume}      255
                    Should contain      ${result}       VOLUME_NOT_FOUND
    ${result} =     Execute and checkrc    ozone sh volume create ${protocol}${server}/${volume} --space-quota 1.5GB      255
                    Should contain      ${result}       1.5GB is invalid
    ${result} =     Execute and checkrc    ozone sh volume create ${protocol}${server}/${volume} --namespace-quota 1.5      255
                    Should contain      ${result}       1.5 is invalid
                    Execute and checkrc    ozone sh volume create ${protocol}${server}/${volume}                            0
    ${result} =     Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket_1                   255
                    Should contain      ${result}       INVALID_BUCKET_NAME
    ${result} =     Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket1 --space-quota 1.5GB    255
                    Should contain      ${result}       1.5GB is invalid
    ${result} =     Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket1 --namespace-quota 1.5    255
                    Should contain      ${result}       1.5 is invalid
    ${result} =     Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket1 --layout Invalid   2
                    Should contain      ${result}       Usage
                    Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket1                    0
    ${result} =     Execute and checkrc    ozone sh key info ${protocol}${server}/${volume}/bucket1/non-existing           255
                    Should contain      ${result}       KEY_NOT_FOUND
    ${result} =     Execute and checkrc    ozone sh key put ${protocol}${server}/${volume}/bucket1/key1 unexisting --type invalid    2
    ${result} =     Execute and checkrc    ozone sh bucket setquota ${volume}/bucket1 --space-quota 1.5                     255
                    Should contain      ${result}       1.5 is invalid
    ${result} =     Execute and checkrc    ozone sh bucket setquota ${volume}/bucket1 --namespace-quota 1.5                 255
                    Should contain      ${result}       1.5 is invalid
    ${result} =     Execute and checkrc    ozone sh volume setquota ${volume} --space-quota 1.5                             255
                    Should contain      ${result}       1.5 is invalid
    ${result} =     Execute and checkrc    ozone sh volume setquota ${volume} --namespace-quota 1.5                         255
                    Should contain      ${result}       1.5 is invalid
                    Execute and checkrc    ozone sh bucket setquota ${volume}/bucket1 --space-quota 2KB                     0
    ${result} =     Execute and checkrc    ozone sh key put ${volume}/bucket1/key1 /opt/hadoop/NOTICE.txt                   255
                    Should contain      ${result}       QUOTA_EXCEEDED
    ${result} =     Execute and checkrc    ozone sh volume setquota ${volume} --space-quota 1KB                             255
                    Should contain      ${result}       QUOTA_EXCEEDED
                    Execute and checkrc    ozone sh bucket clrquota ${volume}/bucket1 --space-quota                         0
    ${result} =     Execute and checkrc    ozone sh volume setquota ${volume} --space-quota 1GB                             255
                    Should contain      ${result}       QUOTA_ERROR
                    Execute and checkrc    ozone sh bucket delete ${protocol}${server}/${volume}/bucket1                    0
                    Execute and checkrc    ozone sh volume setquota ${volume} --space-quota 1GB                             0
    ${result} =     Execute and checkrc    ozone sh bucket create ${protocol}${server}/${volume}/bucket1                    255
                    Should contain      ${result}       QUOTA_ERROR
                    Execute and checkrc    ozone sh volume delete ${protocol}${server}/${volume}                            0

Test Volume Acls
    [arguments]     ${protocol}         ${server}       ${volume}

    Execute Later ozone sh    volume create ${protocol}${server}/${volume}

    ${result} =     Execute Later ozone sh    volume getacl ${protocol}${server}/${volume}
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Not Be Equal    ${EMPTY}

    ${result} =     Execute Later ozone sh    volume addacl ${protocol}${server}/${volume} -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute Later ozone sh    volume getacl ${protocol}${server}/${volume}
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    volume removeacl ${protocol}${server}/${volume} -a user:superuser1:xy
    ${result} =     Execute Later ozone sh    volume getacl ${protocol}${server}/${volume}
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    volume setacl ${protocol}${server}/${volume} -al user:superuser1:rwxy[DEFAULT],group:superuser1:a,user:testuser:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute Later ozone sh    volume getacl ${protocol}${server}/${volume}
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

Test Bucket Acls
    [arguments]     ${protocol}         ${server}       ${volume}

    Execute Later ozone sh    bucket create ${protocol}${server}/${volume}/bb1

    ${result} =     Execute Later ozone sh    bucket getacl ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Not Be Equal    ${EMPTY}

    ${result} =     Execute Later ozone sh    bucket addacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:rwxy
    ${result} =     Execute Later ozone sh    bucket getacl ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    bucket removeacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:xy
    ${result} =     Execute Later ozone sh    bucket getacl ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    bucket setacl ${protocol}${server}/${volume}/bb1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute Later ozone sh    bucket getacl ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${filter} =    Set Variable    jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

Test key handling
    [arguments]     ${protocol}         ${server}       ${volume}

    Put and Get Key Later    ${protocol}${server}/${volume}/bb1/key1    put_from=/opt/hadoop/NOTICE.txt

    Execute Later ozone sh    key put -t RATIS ${protocol}${server}/${volume}/bb1/key1_ratis /opt/hadoop/NOTICE.txt
    ${file2} =    Get Key Later    ${protocol}${server}/${volume}/bb1/key1_ratis
    Assert Later    Compare Files    /opt/hadoop/NOTICE.txt    ${file2}

    Execute Later ozone sh    key cp ${protocol}${server}/${volume}/bb1 key1 key1-copy
    ${file3} =    Get Key Later    ${protocol}${server}/${volume}/bb1/key1-copy
    Assert Later    Compare Files    /opt/hadoop/NOTICE.txt    ${file3}

    ${result} =    Execute Later ozone sh    key info ${protocol}${server}/${volume}/bb1/key1_ratis
    ${filter} =    Set Variable    jq -r '. | select(.name=="key1_ratis")'
    Assert Later    File ${result} Filtered By ${filter} Should    Contain    RATIS

    ${result} =    Execute Later ozone sh    key get ${protocol}${server}/${volume}/bb1/key1 ${file3}
    Assert Later    File ${result} Should    Contain    ${file3} exists

    ${result} =    Execute Later ozone sh    key get --force ${protocol}${server}/${volume}/bb1/key1 ${file3}
    Assert Later    File ${result} Should    Not Contain    ${file3} exists

    ${result} =    Execute Later ozone sh    key put ${protocol}${server}/${volume}/bb1/key1 no-such-file
    Assert Later    File ${result} Should    Contain    File not found: no-such-file

    ${result} =    Execute Later ozone sh    key info ${protocol}${server}/${volume}/bb1/key1
    ${filter} =    Set Variable    jq -r '. | select(.name=="key1")'
    Assert Later    File ${result} Filtered By ${filter} Should
    ...    Contain        creationTime
    ...    Not Contain    ETag

    ${result} =    Execute Later ozone sh    key list ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.name=="key1") | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       key1

    Execute Later ozone sh    key rename ${protocol}${server}/${volume}/bb1 key1 key2
    ${result} =     Execute Later ozone sh    key list ${protocol}${server}/${volume}/bb1
    ${filter} =    Set Variable    jq -r '.[] | select(.name=="key2") | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal       key2

    Execute Later ozone sh    key delete ${protocol}${server}/${volume}/bb1/key2
    Execute Later ozone sh    key delete ${protocol}${server}/${volume}/bb1/key1_ratis
    Execute Later ozone sh    key delete ${protocol}${server}/${volume}/bb1/key1-copy

Test key Acls
    [arguments]     ${protocol}         ${server}       ${volume}

    Execute Later ozone sh    key put ${protocol}${server}/${volume}/bb1/key2 /opt/hadoop/NOTICE.txt

    ${result} =     Execute Later ozone sh    key getacl ${protocol}${server}/${volume}/bb1/key2
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Not Be Equal    ${EMPTY}

    ${result} =     Execute Later ozone sh    key addacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:rwxy
    ${result} =     Execute Later ozone sh    key getacl ${protocol}${server}/${volume}/bb1/key2
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    key removeacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:xy
    ${result} =     Execute Later ozone sh    key getacl ${protocol}${server}/${volume}/bb1/key2
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    key setacl ${protocol}${server}/${volume}/bb1/key2 -al user:superuser1:rwxy,group:superuser1:a,user:testuser:rwxyc
    ${result} =     Execute Later ozone sh    key getacl ${protocol}${server}/${volume}/bb1/key2
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${filter} =    Set Variable    jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

Test prefix Acls
    [arguments]     ${protocol}         ${server}       ${volume}

    Execute Later ozone sh    prefix addacl ${protocol}${server}/${volume}/bb1/prefix1/ -a user:superuser1:rwxy[DEFAULT]

    ${result} =     Execute Later ozone sh    prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    prefix removeacl ${protocol}${server}/${volume}/bb1/prefix1/ -a user:superuser1:xy
    ${result} =     Execute Later ozone sh    prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${result} =     Execute Later ozone sh    prefix setacl ${protocol}${server}/${volume}/bb1/prefix1/ -al user:superuser1:rwxy[DEFAULT],group:superuser1:a[DEFAULT],user:testuser:rwxyc
    ${result} =     Execute Later ozone sh    prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${filter} =    Set Variable    jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    Execute Later ozone sh    key put ${protocol}${server}/${volume}/bb1/prefix1/key1 /opt/hadoop/NOTICE.txt
    ${result} =     Execute Later ozone sh    key getacl ${protocol}${server}/${volume}/bb1/prefix1/key1
    ${filter} =    Set Variable    jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

    ${filter} =    Set Variable    jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Be Equal    superuser1

Test native authorizer
    [arguments]     ${protocol}         ${server}       ${volume}

    Return From Keyword if    '${SECURITY_ENABLED}' == 'false'

    # setup with testuser

    ${expected} =    Get test user principal    testuser
    Should Be Equal    ${TEST_USER}    ${expected}

    # no access
    ${vol1} =    Set Variable    ${protocol}${server}/${volume}1
    Execute Later ozone sh    volume create ${vol1}
    Execute Later ozone sh    volume removeacl ${vol1} -a group:root:a

    # no list
    ${vol2} =    Set Variable    ${protocol}${server}/${volume}2
    Execute Later ozone sh    volume create ${vol2}
    Execute Later ozone sh    volume removeacl ${vol2} -a group:root:a
    Execute Later ozone sh    volume addacl ${vol2} -a user:testuser2:xyrw
    Execute Later ozone sh    bucket create ${vol2}/bb1

    # all access to bucket
    ${vol3} =    Set Variable    ${protocol}${server}/${volume}3
    Execute Later ozone sh    volume create ${vol3}
    Execute Later ozone sh    volume removeacl ${vol3} -a group:root:a
    Execute Later ozone sh    volume addacl ${vol3} -a user:testuser2:xyrw
    Execute Later ozone sh    bucket create ${vol3}/bb1
    Execute Later ozone sh    bucket addacl ${vol3}/bb1 -a user:testuser2:a

    Execute Batch

    # tests with testuser2

    Execute         kdestroy
    Kinit test user     testuser2    testuser2.keytab

    # no access
    ${result} =     Execute Later ozone sh    bucket list ${vol1}
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    key list ${vol1}/bb1
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    volume addacl ${vol1} -a user:testuser2:xy
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED User testuser2 doesn't have WRITE_ACL permission to access volume

    # no list
    ${result} =     Execute Later ozone sh    bucket list ${vol2}
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED User testuser2 doesn't have LIST permission to access volume
    ${result} =     Execute Later ozone sh    volume addacl ${vol2} -a user:testuser2:l
    Assert Later    File ${result} Should    Not Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    bucket list ${vol2}
    Assert Later    File ${result} Should    Not Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    volume getacl ${vol2}
    Assert Later    File ${result} Should    Not Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    key list ${vol2}/bb1
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED

    # all access to bucket
    ${result} =     Execute Later ozone sh    bucket getacl ${vol3}/bb1
    Assert Later    File ${result} Should    Not Contain    PERMISSION_DENIED
    ${result} =     Execute Later ozone sh    key list ${vol3}/bb1
    Assert Later    File ${result} Should    Not Contain    PERMISSION_DENIED

    Execute Batch
    Execute         kdestroy


Test Delete key with Trash
    [arguments]    ${protocol}         ${server}       ${volume}

    Execute Later ozone sh    volume create ${protocol}${server}/${volume}
    Execute Later ozone sh    bucket create ${protocol}${server}/${volume}/bfso --layout FILE_SYSTEM_OPTIMIZED
    Execute Later ozone sh    key put -t RATIS ${protocol}${server}/${volume}/bfso/key3 /opt/hadoop/NOTICE.txt
    Execute Later ozone sh    key delete ${protocol}${server}/${volume}/bfso/key3

    ${result} =    Execute Later ozone sh    key list ${protocol}${server}/${volume}/bfso
    ${filter} =    Set Variable    jq -r '.[] | select(.name | startswith(".Trash")) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Contain          key3

    ${filter} =    Set Variable    jq -r '.[] | select(.name | startswith(".Trash") | not) | .name'
    Assert Later    File ${result} Filtered By ${filter} Should    Not contain      key3

    Execute Later ozone sh    bucket create ${protocol}${server}/${volume}/obsbkt --layout OBJECT_STORE
    Execute Later ozone sh    key put -t RATIS ${protocol}${server}/${volume}/obsbkt/key2 /opt/hadoop/NOTICE.txt
    Execute Later ozone sh    key delete ${protocol}${server}/${volume}/obsbkt/key2

    ${result} =    Execute Later ozone sh    key list ${protocol}${server}/${volume}/obsbkt
    Assert Later    File ${result} Filtered By ${filter} Should    not contain      key2
