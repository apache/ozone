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
Resource            ../ozone-lib/shell.robot
Suite Setup         Get Security Enabled From Config

*** Variables ***
${prefix}    generated
${SCM}       scm
${TMP_JSON}  ${TEMP_DIR}/bb1.json

*** Keywords ***

Generate prefix
   ${random} =         Generate Random String  5  [NUMBERS]
   Set Suite Variable  ${prefix}  ${random}

Test ozone shell
    [arguments]     ${protocol}         ${server}       ${volume}
    ${result} =     Execute and checkrc    ozone sh volume info ${protocol}${server}/${volume}      255
                    Should contain      ${result}       VOLUME_NOT_FOUND
    ${result} =     Execute             ozone sh volume create ${protocol}${server}/${volume} --space-quota 100TB --namespace-quota 100
                    Should Be Empty     ${result}
    ${result} =     Execute             ozone sh volume list ${protocol}${server}/ | jq -r '.[] | select(.name=="${volume}")'
                    Should contain      ${result}       creationTime
# TODO: Disable updating the owner, acls should be used to give access to other user.
                    Execute             ozone sh volume setquota ${protocol}${server}/${volume} --space-quota 10TB --namespace-quota 100
#    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.volumeName=="${volume}") | .owner | .name'
#                    Should Be Equal     ${result}       bill
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       10995116277760
    ${result} =     Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1 --space-quota 10TB --namespace-quota 100
                    Should Be Empty     ${result}
                    Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 > ${TMP_JSON}
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .storageType' ${TMP_JSON}
                    Should Be Equal     ${result}       DISK
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInBytes' ${TMP_JSON}
                    Should Be Equal     ${result}       10995116277760
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInNamespace' ${TMP_JSON}
                    Should Be Equal     ${result}       100
                    Execute             ozone sh bucket setquota ${protocol}${server}/${volume}/bb1 --space-quota 1TB --namespace-quota 1000
                    Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 > ${TMP_JSON}
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInBytes' ${TMP_JSON}
                    Should Be Equal     ${result}       1099511627776
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInNamespace' ${TMP_JSON}
                    Should Be Equal     ${result}       1000
    ${result} =     Execute             ozone sh bucket list ${protocol}${server}/${volume}/ | jq -r '.[] | select(.name=="bb1") | .volumeName'
                    Should Be Equal     ${result}       ${volume}
                    Run Keyword         Test key handling       ${protocol}       ${server}       ${volume}
                    Execute             ozone sh volume clrquota --space-quota ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh volume clrquota --namespace-quota ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume info ${protocol}${server}/${volume} | jq -r '. | select(.name=="${volume}") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket clrquota --space-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInBytes'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket clrquota --namespace-quota ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 | jq -r '. | select(.name=="bb1") | .quotaInNamespace'
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket delete -r --yes ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh volume delete ${protocol}${server}/${volume}
                    Execute             ozone sh volume create ${protocol}${server}/${volume}
                    Execute             ozone sh volume info ${protocol}${server}/${volume} > ${TMP_JSON}
    ${result} =     Execute             jq -r '. | select(.name=="${volume}") | .quotaInBytes' ${TMP_JSON}
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             jq -r '. | select(.name=="${volume}") | .quotaInNamespace' ${TMP_JSON}
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket create ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh bucket info ${protocol}${server}/${volume}/bb1 > ${TMP_JSON}
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInBytes' ${TMP_JSON}
                    Should Be Equal     ${result}       -1
    ${result} =     Execute             jq -r '. | select(.name=="bb1") | .quotaInNamespace' ${TMP_JSON}
                    Should Be Equal     ${result}       -1
                    Execute             ozone sh bucket delete ${protocol}${server}/${volume}/bb1
                    Execute             ozone sh volume delete ${protocol}${server}/${volume}

Test ozone shell errors
    [arguments]     ${protocol}         ${server}       ${volume}
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
    Execute         ozone sh volume create ${protocol}${server}/${volume}
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Should Not Be Empty    ${acl_check}
    ${result} =     Execute             ozone sh volume addacl ${protocol}${server}/${volume} -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh volume removeacl ${protocol}${server}/${volume} -a user:superuser1:xy
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh volume setacl ${protocol}${server}/${volume} -al user:superuser1:rwxy[DEFAULT],group:superuser1:a,user:testuser:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh volume getacl ${protocol}${server}/${volume}
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1

Test Bucket Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh bucket create ${protocol}${server}/${volume}/bb1
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Should Not Be Empty    ${acl_check}
    ${result} =     Execute             ozone sh bucket addacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh bucket removeacl ${protocol}${server}/${volume}/bb1 -a user:superuser1:xy
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh bucket setacl ${protocol}${server}/${volume}/bb1 -al user:superuser1:rwxy,group:superuser1:a,user:testuser:rwxyc,group:superuser1:a[DEFAULT]
    ${result} =     Execute             ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1

Test key handling
    [arguments]     ${protocol}         ${server}       ${volume}
                    Execute             rm -f /tmp/NOTICE.txt.1 /tmp/key1_RATIS /tmp/key1-copy
                    Ozone Shell Batch   key put ${protocol}${server}/${volume}/bb1/key1 /opt/hadoop/NOTICE.txt
                    ...                 key get ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    ...                 key put -t RATIS ${protocol}${server}/${volume}/bb1/key1_RATIS /opt/hadoop/NOTICE.txt
                    ...                 key get ${protocol}${server}/${volume}/bb1/key1_RATIS /tmp/key1_RATIS
                    ...                 key cp ${protocol}${server}/${volume}/bb1 key1 key1-copy
                    ...                 key get ${protocol}${server}/${volume}/bb1/key1-copy /tmp/key1-copy
                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/NOTICE.txt.1
                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/key1_RATIS

    ${result} =     Execute             ozone sh key info ${protocol}${server}/${volume}/bb1/key1_RATIS | jq -r '. | select(.name=="key1_RATIS")'
                    Should contain      ${result}       RATIS

                    Execute             diff -q /opt/hadoop/NOTICE.txt /tmp/key1-copy

    ${result} =     Execute And Ignore Error    ozone sh key get ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    Should Contain      ${result}       NOTICE.txt.1 exists
    ${result} =     Execute             ozone sh key get --force ${protocol}${server}/${volume}/bb1/key1 /tmp/NOTICE.txt.1
                    Should Not Contain  ${result}       NOTICE.txt.1 exists
    ${result} =     Execute and checkrc    ozone sh key put ${protocol}${server}/${volume}/bb1/key1 sample.txt          255
                    Should Contain         ${result}       File not found: sample.txt
    ${result} =     Execute             ozone sh key info ${protocol}${server}/${volume}/bb1/key1 | jq -r '. | select(.name=="key1")'
                    Should contain      ${result}       creationTime
                    Should not contain  ${result}       ETag
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | jq -r '.[] | select(.name=="key1") | .name'
                    Should Be Equal     ${result}       key1
                    Execute             ozone sh key rename ${protocol}${server}/${volume}/bb1 key1 key2
    ${result} =     Execute             ozone sh key list ${protocol}${server}/${volume}/bb1 | jq -r '.[] | select(.name=="key2") | .name'
                    Should Be Equal     ${result}       key2
                    Ozone Shell Batch   key delete ${protocol}${server}/${volume}/bb1/key2
                    ...                 key delete ${protocol}${server}/${volume}/bb1/key1_RATIS
                    ...                 key delete ${protocol}${server}/${volume}/bb1/key1-copy

Test key Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh key put ${protocol}${server}/${volume}/bb1/key2 /opt/hadoop/NOTICE.txt
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Should Not Be Empty    ${acl_check}
    ${result} =     Execute             ozone sh key addacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:rwxy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh key removeacl ${protocol}${server}/${volume}/bb1/key2 -a user:superuser1:xy
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh key setacl ${protocol}${server}/${volume}/bb1/key2 -al user:superuser1:rwxy,group:superuser1:a,user:testuser:rwxyc
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/key2
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1

Test prefix Acls
    [arguments]     ${protocol}         ${server}       ${volume}
    Execute         ozone sh prefix addacl ${protocol}${server}/${volume}/bb1/prefix1/ -a user:superuser1:rwxy[DEFAULT]
    ${result} =     Execute             ozone sh prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh prefix removeacl ${protocol}${server}/${volume}/bb1/prefix1/ -a user:superuser1:xy
    ${result} =     Execute             ozone sh prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${result} =     Execute             ozone sh prefix setacl ${protocol}${server}/${volume}/bb1/prefix1/ -al user:superuser1:rwxy[DEFAULT],group:superuser1:a[DEFAULT],user:testuser:rwxyc
    ${result} =     Execute             ozone sh prefix getacl ${protocol}${server}/${volume}/bb1/prefix1/
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="DEFAULT" and (.aclList | contains(["ALL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    Execute         ozone sh key put ${protocol}${server}/${volume}/bb1/prefix1/key1 /opt/hadoop/NOTICE.txt
    ${result} =     Execute             ozone sh key getacl ${protocol}${server}/${volume}/bb1/prefix1/key1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="USER" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["READ", "WRITE", "READ_ACL", "WRITE_ACL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1
    ${acl_check} =  Execute             echo '${result}' | jq -r '.[] | select(.type=="GROUP" and .name=="superuser1" and .aclScope=="ACCESS" and (.aclList | contains(["ALL"]))) | .name'
    Should Be Equal    ${acl_check}    superuser1

Test native authorizer
    [arguments]     ${protocol}         ${server}       ${volume}

    Return From Keyword if    '${SECURITY_ENABLED}' == 'false'

    Execute         ozone sh volume removeacl ${protocol}${server}/${volume} -a group:root:a
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    ${result} =     Execute And Ignore Error         ozone sh bucket list ${protocol}${server}/${volume}
                    Should contain      ${result}    PERMISSION_DENIED
    ${result} =     Execute And Ignore Error         ozone sh key list ${protocol}${server}/${volume}/bb1
                    Should contain      ${result}    PERMISSION_DENIED
    ${result} =     Execute And Ignore Error         ozone sh volume addacl ${protocol}${server}/${volume} -a user:testuser2:xy
                    Should contain      ${result}    PERMISSION_DENIED User testuser2 doesn't have WRITE_ACL permission to access volume
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser     testuser.keytab
    Execute         ozone sh volume addacl ${protocol}${server}/${volume} -a user:testuser2:xyrw
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    ${result} =     Execute And Ignore Error         ozone sh bucket list ${protocol}${server}/${volume}
                    Should contain      ${result}    PERMISSION_DENIED User testuser2 doesn't have LIST permission to access volume
    Execute         ozone sh volume addacl ${protocol}${server}/${volume} -a user:testuser2:l
    Execute         ozone sh bucket list ${protocol}${server}/${volume}
    Execute         ozone sh volume getacl ${protocol}${server}/${volume}

    ${result} =     Execute And Ignore Error         ozone sh key list ${protocol}${server}/${volume}/bb1
    Should contain      ${result}    PERMISSION_DENIED
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser     testuser.keytab
    Execute         ozone sh bucket addacl ${protocol}${server}/${volume}/bb1 -a user:testuser2:a
    Execute         ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser2    testuser2.keytab
    Execute         ozone sh bucket getacl ${protocol}${server}/${volume}/bb1
    Execute         ozone sh key list ${protocol}${server}/${volume}/bb1
    Execute         kdestroy
    Run Keyword     Kinit test user     testuser    testuser.keytab

Test Delete key with Trash
    [arguments]    ${protocol}         ${server}       ${volume}
                   Ozone Shell Batch     volume create ${protocol}${server}/${volume}
                   ...                   bucket create ${protocol}${server}/${volume}/bfso --layout FILE_SYSTEM_OPTIMIZED
                   ...                   key put -t RATIS ${protocol}${server}/${volume}/bfso/key3 /opt/hadoop/NOTICE.txt
                   ...                   key delete ${protocol}${server}/${volume}/bfso/key3
    ${fsokey} =    Execute               ozone sh key list ${protocol}${server}/${volume}/bfso
    ${result} =    Execute               echo '${fsokey}' | jq -r '.[] | select(.name | startswith(".Trash")) | .name'
                   Should Contain Any    ${result}    .Trash/hadoop    .Trash/testuser    .Trash/root
                   Should contain        ${result}    key3
    ${result} =    Execute               echo '${fsokey}' | jq -r '.[] | select(.name | startswith(".Trash") | not) | .name'
                   Should Not contain    ${result}    key3
                   Ozone Shell Batch     bucket create ${protocol}${server}/${volume}/obsbkt --layout OBJECT_STORE
                   ...                   key put -t RATIS ${protocol}${server}/${volume}/obsbkt/key2 /opt/hadoop/NOTICE.txt
                   ...                   key delete ${protocol}${server}/${volume}/obsbkt/key2
    ${result} =    Execute               ozone sh key list ${protocol}${server}/${volume}/obsbkt
                   Should not contain    ${result}    key2
