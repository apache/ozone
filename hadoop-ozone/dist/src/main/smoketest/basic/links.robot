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
Resource            ../ozone-lib/batch.robot
Resource            ../ozone-lib/batch-shell.robot
Test Timeout        4 minute
Suite Setup         Setup Suite
Suite Teardown      Teardown Suite

*** Variables ***
${prefix}    generated
${SCM}       scm

*** Keywords ***
Setup Suite
    Get Security Enabled From Config
    Setup Batch
    Kinit test user
    Setup Tests
    Setup Secure Tests

Teardown Suite
    Teardown Batch

Setup Tests
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${source}  ${random}-source
    Set Suite Variable  ${target}  ${random}-target
    Execute Later ozone sh    volume create ${source}
    Execute Later ozone sh    volume create ${target}
    Execute Later ozone sh    bucket link ${source}/no-such-bucket ${target}/dangling-link
    Execute Later ozone sh    bucket link no-such-volume/no-such-bucket ${target}/dangling-missing-vol
    Execute Later ozone sh    bucket link ${target}/dangling-missing-vol ${target}/link-to-dangling-link
    Execute Later ozone sh    bucket link ${source}/bucket-in-source ${target}/link-to-existing-bucket
    Execute Later ozone sh    bucket create ${source}/bucket-in-source
    Execute Later ozone sh    bucket create ${target}/bucket-in-target

Setup Secure Tests
    Return From Keyword If   '${SECURITY_ENABLED}' != 'true'    only if security is enabled

    Execute Later ozone sh    bucket create ${source}/readable-bucket
    Execute Later ozone sh    key put ${source}/readable-bucket/key-in-readable-bucket /etc/passwd
    Execute Later ozone sh    bucket create ${source}/unreadable-bucket
    Execute Later ozone sh    bucket link ${source}/unreadable-bucket ${target}/link-to-unreadable-bucket
    Execute Later ozone sh    volume addacl --acl user:testuser2:r[DEFAULT] ${target}
    Execute Later ozone sh    bucket link ${source}/readable-bucket ${target}/readable-link
    Execute Later ozone sh    volume addacl --acl user:testuser2:rl ${source}
    Execute Later ozone sh    bucket addacl --acl user:testuser2:rl ${source}/readable-bucket

Verify ACL in ${file}
    [arguments]    ${type}    ${name}    ${expected}
    ${actual} =    Execute    jq -r '.[] | select(.type == "${type}") | select(.name == "${name}") | .aclList[]' ${file} | xargs
    Should Be Equal    ${actual}    ${expected}

Verify ACL of ${object_type} ${object}
    [arguments]   @{params}    ${source_option}=${EMPTY}
    ${output_file} =    Execute Later ozone sh    ${object_type} getacl ${source_option} ${object}
    Assert Later    Verify ACL in ${output_file}    @{params}

Can follow link with read access
    ${result} =    Execute Later ozone sh    key list ${target}/readable-link
    Assert Later    File ${result} Should    Contain    key-in-readable-bucket

Cannot follow link without read access
    ${result} =    Execute Later ozone sh    key list ${target}/link-to-unreadable-bucket
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED

    ${result} =    Execute Later ozone sh    key list ${source}/unreadable-bucket
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED

ACL verified on source and target bucket
    ${result} =    Execute Later ozone sh    bucket info ${target}/link-to-unreadable-bucket
    Assert Later    File ${result} Should
    ...    Contain    link-to-unreadable-bucket
    ...    Not Contain    PERMISSION_DENIED

    ${result} =    Execute Later ozone sh    bucket info ${source}/unreadable-bucket
    Assert Later    File ${result} Should    Contain    PERMISSION_DENIED

Create link loop
    Execute Later ozone sh    bucket link ${target}/loop1 ${target}/loop2
    Execute Later ozone sh    bucket link ${target}/loop2 ${target}/loop3
    Execute Later ozone sh    bucket link ${target}/loop3 ${target}/loop1

Delete link loop
    Execute Later ozone sh    bucket delete ${target}/loop1
    Execute Later ozone sh    bucket delete ${target}/loop2
    Execute Later ozone sh    bucket delete ${target}/loop3


*** Test Cases ***
Link to non-existent bucket
    ${result} =    Execute Later ozone sh    key list ${target}/dangling-link
    Assert Later    File ${result} Should
    ...    Contain    BUCKET_NOT_FOUND
    ...    Contain    Cannot follow bucket link

Link to non-existent source volume
    ${result} =    Execute Later ozone sh    key list ${target}/dangling-missing-vol
    Assert Later    File ${result} Should
    ...    Contain    VOLUME_NOT_FOUND
    ...    Contain    Cannot follow bucket link

Link chain to broken source
    ${result} =    Execute Later ozone sh    key list ${target}/link-to-dangling-link
    Assert Later    File ${result} Should
    ...    Contain    VOLUME_NOT_FOUND
    ...    Contain    Cannot follow bucket link

Key create passthrough
    # put to link, get from bucket
    Put and Get Key Later    put_to=${target}/link-to-existing-bucket/key1    get_from=${source}/bucket-in-source/key1

Key read passthrough
    # put to bucket, get from link
    Put and Get Key Later    put_to=${source}/bucket-in-source/key2    get_from=${target}/link-to-existing-bucket/key2

Key list passthrough
    ${target_list} =    Execute Later ozone sh    key list ${target}/link-to-existing-bucket
    ${source_list} =    Execute Later ozone sh    key list ${source}/bucket-in-source
    Assert Later    Compare files    ${source_list}    ${target_list}
    Assert Later    File ${source_list} Should
    ...    Contain    key1
    ...    Contain    key2

Key delete passthrough
    Execute Later ozone sh    key delete ${target}/link-to-existing-bucket/key2
    ${delete} =    Execute Later ozone sh    key list --all --prefix .Trash ${source}/bucket-in-source
    ${retain} =    Execute Later ozone sh    key list --all --prefix key ${source}/bucket-in-source
    Assert Later   File ${delete} Should    Contain    key2
    Assert Later   File ${retain} Should    Not Contain    key2

Bucket list contains links
    ${result} =    Execute Later ozone sh    bucket list ${target}
    Assert Later    File ${result} Should
    ...    Contain    link-to-existing-bucket
    ...    Contain    dangling-link

Bucket info shows source
    ${result} =    Execute Later ozone sh    bucket info ${target}/link-to-existing-bucket
    Assert Later    File ${result} Should
    ...    Contain    ${source}
    ...    Contain    bucket-in-source
    ...    Contain    ${target}
    ...    Contain    link-to-existing-bucket
    ...    Contain    modificationTime
    ...    Contain    creationTime
    ...    Not Contain    metadata

Source and target bucket have different ACLs
    Execute Later ozone sh    bucket addacl --acl user:user1:rwxy ${target}/link-to-existing-bucket
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    ${EMPTY}
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    ${EMPTY}    source_option=--source=false
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    READ WRITE READ_ACL WRITE_ACL    source_option=--source
    Verify ACL of bucket ${source}/bucket-in-source    USER    user1    READ WRITE READ_ACL WRITE_ACL

    Execute Later ozone sh    bucket removeacl --acl user:user1:y ${target}/link-to-existing-bucket
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    ${EMPTY}
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    READ WRITE READ_ACL    source_option=--source

    Execute Later ozone sh    bucket setacl --acl user:user1:rw ${source}/bucket-in-source
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    ${EMPTY}
    Verify ACL of bucket ${target}/link-to-existing-bucket    USER    user1    READ WRITE    source_option=--source

    Execute Later ozone sh    bucket addacl --acl group:group2:r ${source}/bucket-in-source
    Verify ACL of bucket ${target}/link-to-existing-bucket    GROUP   group2    ${EMPTY}
    Verify ACL of bucket ${target}/link-to-existing-bucket    GROUP   group2    READ    source_option=--source

Source and target key have same ACLs
    Execute Later ozone sh    key addacl --acl user:user1:rwxy ${source}/bucket-in-source/key1
    Verify ACL of key ${target}/link-to-existing-bucket/key1    USER    user1    READ WRITE READ_ACL WRITE_ACL
    Verify ACL of key ${source}/bucket-in-source/key1   USER    user1    READ WRITE READ_ACL WRITE_ACL

    Execute Later ozone sh    key removeacl --acl user:user1:y ${target}/link-to-existing-bucket/key1
    Verify ACL of key ${target}/link-to-existing-bucket/key1    USER    user1    READ WRITE READ_ACL
    Verify ACL of key ${source}/bucket-in-source/key1   USER    user1    READ WRITE READ_ACL

    Execute Later ozone sh    key setacl --acl user:user1:rw ${source}/bucket-in-source/key1
    Verify ACL of key ${target}/link-to-existing-bucket/key1    USER    user1    READ WRITE
    Verify ACL of key ${source}/bucket-in-source/key1   USER    user1    READ WRITE

    Execute Later ozone sh    key addacl --acl group:group2:r ${source}/bucket-in-source/key1
    Verify ACL of key ${target}/link-to-existing-bucket/key1    GROUP   group2    READ
    Verify ACL of key ${source}/bucket-in-source/key1   GROUP   group2    READ

Source and target prefix have same ACLs
    Execute Later ozone sh    prefix addacl --acl user:user1:rwxy ${source}/bucket-in-source/prefix1/
    Verify ACL of prefix ${target}/link-to-existing-bucket/prefix1/    USER    user1    READ WRITE READ_ACL WRITE_ACL
    Verify ACL of prefix ${source}/bucket-in-source/prefix1/   USER    user1    READ WRITE READ_ACL WRITE_ACL

    Execute Later ozone sh    prefix removeacl --acl user:user1:y ${target}/link-to-existing-bucket/prefix1/
    Verify ACL of prefix ${target}/link-to-existing-bucket/prefix1/    USER    user1    READ WRITE READ_ACL
    Verify ACL of prefix ${source}/bucket-in-source/prefix1/   USER    user1    READ WRITE READ_ACL

    Execute Later ozone sh    prefix setacl --acl user:user1:rw ${source}/bucket-in-source/prefix1/
    Verify ACL of prefix ${target}/link-to-existing-bucket/prefix1/    USER    user1    READ WRITE
    Verify ACL of prefix ${source}/bucket-in-source/prefix1/   USER    user1    READ WRITE

    Execute Later ozone sh    prefix addacl --acl group:group2:r ${source}/bucket-in-source/prefix1/
    Verify ACL of prefix ${target}/link-to-existing-bucket/prefix1/    GROUP   group2    READ
    Verify ACL of prefix ${source}/bucket-in-source/prefix1/   GROUP   group2    READ

Buckets and links share namespace
    ${result} =    Execute Later ozone sh    bucket create ${target}/link-to-existing-bucket
    Assert Later    File ${result} Should    Contain    BUCKET_ALREADY_EXISTS

    ${result} =    Execute Later ozone sh    bucket create ${target}/dangling-link
    Assert Later    File ${result} Should    Contain    BUCKET_ALREADY_EXISTS

    ${result} =    Execute Later ozone sh    bucket link ${source}/bucket-in-source ${target}/bucket-in-target
    Assert Later    File ${result} Should    Contain    BUCKET_ALREADY_EXISTS

Loop in link chain is detected
    [setup]    Create link loop
    ${result} =    Execute Later ozone sh    key list ${target}/loop2
    Assert Later    File ${result} Should    Contain    DETECTED_LOOP
    [teardown]    Delete link loop

Multiple links to same bucket are allowed
    Execute Later ozone sh    bucket link ${source}/bucket-in-source ${target}/another-link
    Put and Get Key Later    put_to=${target}/another-link/key3    get_from=${target}/link-to-existing-bucket/key3    put_from=/etc/group
    Put and Get Key Later    put_to=${target}/link-to-existing-bucket/key4    get_from=${target}/another-link/key4    put_from=/etc/hostname

Source bucket not affected by deleting link
    Execute Later ozone sh    bucket link ${source}/bucket-in-source ${target}/link-to-be-deleted
    Execute Later ozone sh    bucket delete ${target}/link-to-be-deleted

    ${buckets_in_target} =    Execute Later ozone sh    bucket list ${target}
    Assert Later    File ${buckets_in_target} Should    Not Contain    link-to-be-deleted

    ${keys_in_source} =    Execute Later ozone sh    key list ${source}/bucket-in-source
    Assert Later    File ${keys_in_source} Should    Contain    key3

Setting bucket property on link not allowed
    ${result} =    Execute Later ozone sh    bucket setquota ${target}/link-to-existing-bucket --quota 1GB
    Assert Later    File ${result} Should    Contain    NOT_SUPPORTED_OPERATION

Bucket info for link contains source
    ${result} =    Execute Later ozone sh    bucket info ${target}/link-to-existing-bucket
    Assert Later    File ${result} Should    Contain    sourceBucket

Execute Batch for testuser
    Execute Batch

Test with other user
    Skip If   '${SECURITY_ENABLED}' != 'true'    only if security is enabled

    Execute    kdestroy
    Run Keyword    Kinit test user    testuser2    testuser2.keytab

    Can follow link with read access
    Cannot follow link without read access
    ACL verified on source and target bucket

    Execute Batch
