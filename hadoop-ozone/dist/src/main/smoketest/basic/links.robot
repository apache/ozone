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
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        2 minute
Suite Setup         Create volumes

*** Variables ***
${prefix}    generated

*** Keywords ***
Create volumes
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${source}  ${random}-source
    Set Suite Variable  ${target}  ${random}-target
    Execute             ozone sh volume create ${source}
    Execute             ozone sh volume create ${target}

*** Test Cases ***
Link to non-existent bucket
                        Execute                     ozone sh bucket link ${source}/no-such-bucket ${target}/dangling-link
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/dangling-link
                        Should Contain              ${result}         BUCKET_NOT_FOUND

Key create passthrough
                        Execute                     ozone sh bucket link ${source}/bucket1 ${target}/link1
                        Execute                     ozone sh bucket create ${source}/bucket1
                        Execute                     ozone sh key put ${target}/link1/key1 /etc/passwd
                        Should Match Local File     ${target}/link1/key1    /etc/passwd

Key read passthrough
                        Execute                     ozone sh key put ${source}/bucket1/key2 /opt/hadoop/NOTICE.txt
                        Should Match Local File     ${source}/bucket1/key2    /opt/hadoop/NOTICE.txt

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

Source and target have separate ACLs
    Verify ACL    bucket    ${target}/link1      USER    hadoop    ALL
    Verify ACL    bucket    ${source}/bucket1    USER    hadoop    ALL

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

Loop in link chain is detected
                        Execute                     ozone sh bucket link ${target}/loop1 ${target}/loop2
                        Execute                     ozone sh bucket link ${target}/loop2 ${target}/loop3
                        Execute                     ozone sh bucket link ${target}/loop3 ${target}/loop1
    ${result} =         Execute And Ignore Error    ozone sh key list ${target}/loop2
                        Should Contain              ${result}    INTERNAL_ERROR

Multiple links to same bucket are allowed
                        Execute                     ozone sh bucket link ${source}/bucket1 ${target}/link3
                        Execute                     ozone sh key put ${target}/link3/key3 /etc/group
                        Should Match Local File     ${target}/link1/key3    /etc/group

Source bucket not affected by deleting link
                        Execute                     ozone sh bucket delete ${target}/link1
    ${bucket_list} =    Execute                     ozone sh bucket list ${target}
                        Should Not Contain          ${bucket_list}    link1
    ${source_list} =    Execute                     ozone sh key list ${source}/bucket1 | jq -r '.name'
                        Should Contain              ${source_list}    key1
