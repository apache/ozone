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
Documentation       Test for reading snapshots with ACLs as different users
Library             OperatingSystem
Resource            snapshot-setup.robot
Test Timeout        5 minutes
Suite Setup         Run Keywords       Get Security Enabled From Config
...    AND          Run Keyword if  '${SECURITY_ENABLED}' == 'false'    BuiltIn.Skip

*** Variables ***
${USER1} =              testuser
${USER2} =              testuser2
${KEY}
${FIRST_SNAPSHOT}
${SECOND_SNAPSHOT}

*** Keywords ***
Add ACL
    [arguments]         ${object}   ${user}     ${objectName}
    ${result} =         Execute     ozone sh ${object} addacl -a user:${user}:a ${objectName}
    Should not contain  ${result}   PERMISSION_DENIED

Add ACLs
    Add ACL     volume      ${USER2}    ${VOLUME}
    Add ACL     bucket      ${USER2}    ${VOLUME}/${BUCKET}
    Add ACL     key         ${USER2}    ${VOLUME}/${BUCKET}/${KEY}

Get key
    [arguments]     ${snapshotName}                 ${keyDest}
    ${result} =     Execute And Ignore Error        ozone sh key get ${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${snapshotName}/${KEY} ${keyDest}
    [return]        ${result}

*** Test Cases ***
Test creating first snapshot as user1
    Execute                                 kdestroy
    Kinit test user                         ${USER1}            ${USER1}.keytab
    Setup volume and bucket
    ${key} =            Create key          ${VOLUME}           ${BUCKET}   README.md
    Set Suite Variable  ${KEY}              ${key}
    ${snapshot} =       Create snapshot     ${VOLUME}           ${BUCKET}
    Set Suite Variable                      ${FIRST_SNAPSHOT}   ${snapshot}

Test adding ACLs for user2
    Add ACLs

Test creating second snapshot as user2
    ${snapshot} =       Create snapshot     ${VOLUME}   ${BUCKET}
    Set Suite Variable  ${SECOND_SNAPSHOT}  ${snapshot}

Test reading first snapshot as user2
    Execute             kdestroy
    Kinit test user     ${USER2}                ${USER2}.keytab
    ${keyDest} =        Generate Random String  5   [LOWER]
    ${result} =         Get key                 ${FIRST_SNAPSHOT}     ${TEMP_DIR}/${keyDest}
    Should contain      ${result}               PERMISSION_DENIED

Test reading second snapshot as user2
    ${keyDest} =            Generate Random String  5   [LOWER]
    ${result} =             Get key                 ${SECOND_SNAPSHOT}     ${TEMP_DIR}/${keyDest}
    Should not contain      ${result}               PERMISSION_DENIED
