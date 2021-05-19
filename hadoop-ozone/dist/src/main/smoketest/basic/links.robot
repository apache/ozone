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
Documentation       Test bucket links via Ozone CLI
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        2 minute
Suite Setup         Create volumes

*** Variables ***
${prefix}    generated
${SCM}       scm

*** Keywords ***
Create volumes
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${source}  ${random}-source
    Set Suite Variable  ${target}  ${random}-target
    Execute             ozone sh volume create ${source}
    Execute             ozone sh volume create ${target}
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'    Setup ACL tests

Setup ACL tests
    Execute             ozone sh bucket create ${source}/readable-bucket
    Execute             ozone sh key put ${source}/readable-bucket/key-in-readable-bucket /etc/passwd
    Execute             ozone sh bucket create ${source}/unreadable-bucket
    Execute             ozone sh bucket link ${source}/readable-bucket ${target}/readable-link
    Execute             ozone sh bucket link ${source}/readable-bucket ${target}/unreadable-link
    Execute             ozone sh bucket link ${source}/unreadable-bucket ${target}/link-to-unreadable-bucket
    Execute             ozone sh volume addacl --acl user:testuser2/scm@EXAMPLE.COM:r ${target}
    Execute             ozone sh volume addacl --acl user:testuser2/scm@EXAMPLE.COM:rl ${source}
    Execute             ozone sh bucket addacl --acl user:testuser2/scm@EXAMPLE.COM:rl ${source}/readable-bucket
    Execute             ozone sh bucket addacl --acl user:testuser2/scm@EXAMPLE.COM:r ${target}/readable-link
    Execute             ozone sh bucket addacl --acl user:testuser2/scm@EXAMPLE.COM:r ${target}/link-to-unreadable-bucket

Can follow link with read access
    Execute             kdestroy
    Run Keyword         Kinit test user             testuser2         testuser2.keytab
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/readable-link
                        Should Contain              ${result}         key-in-readable-bucket

Cannot follow link without read access
    Execute             kdestroy
    Run Keyword         Kinit test user             testuser2         testuser2.keytab
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/unreadable-link
                        Should Contain              ${result}         PERMISSION_DENIED

ACL verified on source bucket
    Execute             kdestroy
    Run Keyword         Kinit test user             testuser2         testuser2.keytab
    ${result} =         Execute                     ozone sh bucket info ${target}/link-to-unreadable-bucket
                        Should Contain              ${result}         link-to-unreadable-bucket
                        Should Not Contain          ${result}         PERMISSION_DENIED
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/link-to-unreadable-bucket
                        Should Contain              ${result}         PERMISSION_DENIED

*** Test Cases ***
Link to non-existent bucket
                        Execute                     ozone sh bucket link ${source}/no-such-bucket ${target}/dangling-link
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/dangling-link
                        Should Contain              ${result}         BUCKET_NOT_FOUND

Key create passthrough
                        Execute                     ozone sh bucket link ${source}/bucket1 ${target}/link1
                        Execute                     ozone sh bucket create ${source}/bucket1
                        Execute                     ozone sh key put ${target}/link1/key1 /etc/passwd
                        Key Should Match Local File     ${target}/link1/key1    /etc/passwd

Key read passthrough
                        Execute                     ozone sh key put ${source}/bucket1/key2 /opt/hadoop/NOTICE.txt
                        Key Should Match Local File     ${source}/bucket1/key2    /opt/hadoop/NOTICE.txt

Key list passthrough
    ${target_list} =    Execute                     ozone sh key list ${target}/link1 | jq -r '.name'
    ${source_list} =    Execute                     ozone sh key list ${source}/bucket1 | jq -r '.name'
                        Should Be Equal             ${target_list}    ${source_list}
                        Should Contain              ${source_list}    key1
                        Should Contain              ${source_list}    key2

Key delete passthrough
                        Execute                     ozone sh key delete ${target}/link1/key2
    ${source_list} =    Execute                     ozone sh key list ${source}/bucket1 | jq -r '.name'
                        Should Not Contain          ${source_list}    key2

Bucket list contains links
    ${result} =         Execute                     ozone sh bucket list ${target}
                        Should Contain              ${result}         link1
                        Should Contain              ${result}         dangling-link

Bucket info shows source
    ${result} =         Execute                     ozone sh bucket info ${target}/link1 | jq -r '.sourceVolume, .sourceBucket' | xargs
                        Should Be Equal             ${result}    ${source} bucket1

Source and target have separate ACLs
    Execute       ozone sh bucket addacl --acl user:user1:rwxy ${target}/link1
    Verify ACL    bucket    ${target}/link1      USER    user1    READ WRITE READ_ACL WRITE_ACL
    Verify ACL    bucket    ${source}/bucket1    USER    user1    ${EMPTY}

    Execute       ozone sh bucket addacl --acl group:group2:r ${source}/bucket1
    Verify ACL    bucket    ${target}/link1      GROUP   group2    ${EMPTY}
    Verify ACL    bucket    ${source}/bucket1    GROUP   group2    READ

Buckets and links share namespace
                        Execute                     ozone sh bucket link ${source}/bucket2 ${target}/link2
    ${result} =         Execute And Ignore Error    ozone sh bucket create ${target}/link2
                        Should Contain              ${result}    BUCKET_ALREADY_EXISTS

                        Execute                     ozone sh bucket create ${target}/bucket3
    ${result} =         Execute And Ignore Error    ozone sh bucket link ${source}/bucket1 ${target}/bucket3
                        Should Contain              ${result}    BUCKET_ALREADY_EXISTS

Can follow link with read access
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Can follow link with read access

Cannot follow link without read access
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Cannot follow link without read access

ACL verified on source bucket
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    ACL verified on source bucket

Loop in link chain is detected
                        Execute                     ozone sh bucket link ${target}/loop1 ${target}/loop2
                        Execute                     ozone sh bucket link ${target}/loop2 ${target}/loop3
                        Execute                     ozone sh bucket link ${target}/loop3 ${target}/loop1
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/loop2
                        Should Contain              ${result}    DETECTED_LOOP

Multiple links to same bucket are allowed
    Execute                         ozone sh bucket link ${source}/bucket1 ${target}/link3
    Execute                         ozone sh key put ${target}/link3/key3 /etc/group
    Key Should Match Local File     ${target}/link1/key3    /etc/group

Source bucket not affected by deleting link
                        Execute                     ozone sh bucket delete ${target}/link1
    ${bucket_list} =    Execute                     ozone sh bucket list ${target}
                        Should Not Contain          ${bucket_list}    link1
    ${source_list} =    Execute                     ozone sh key list ${source}/bucket1 | jq -r '.name'
                        Should Contain              ${source_list}    key1
