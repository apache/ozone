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
Documentation       HttpFS gateway test with curl commands
Library             Process
Library             String
Library             BuiltIn
Library             OperatingSystem
Resource            operations.robot
Resource            ../lib/os.robot
Resource            ../commonlib.robot
Suite Setup         Run Keywords    Generate volume 
...                 AND             Get Security Enabled From Config

Test Timeout        2 minutes

*** Variables ***
${volume}                      generated

*** Keywords ***
Generate volume
   ${random} =         Generate Random String  5  [LOWER]
   Set Suite Variable  ${volume}  ${random}

Kinit admin
    Wait Until Keyword Succeeds      2min       10sec      Execute      kinit -k om/om@EXAMPLE.COM -t /etc/security/keytabs/om.keytab

*** Test Cases ***
Kinit admin user
    Pass Execution If       '${SECURITY_ENABLED}'=='false'       This is for secured environment
    Kinit admin

Create volume
    ${vol} =     Execute curl command    ${volume}    MKDIRS      -X PUT
    Should contain  ${vol.stdout}   true

Set owner of volume
    Pass Execution If       '${SECURITY_ENABLED}'=='false'       This is for secured environment
    ${rc} =                             Run And Return Rc       ozone sh volume update --user=testuser /${volume}
    Should Be Equal As Integers         ${rc}       0

Kinit testuser
    Pass Execution If       '${SECURITY_ENABLED}'=='false'       This is for secured environment
    Kinit test user     testuser     testuser.keytab

Create first bucket
    ${bucket} =     Execute curl command    ${volume}/buck1          MKDIRS      -X PUT
    Should contain  ${bucket.stdout}   true

Create second bucket
    ${bucket} =     Execute curl command    ${volume}/buck2          MKDIRS      -X PUT
    Should contain  ${bucket.stdout}   true

Create local testfile
    Create File       ${TEMP_DIR}/testfile    "Hello world!"

Create testfile
    ${file} =       Execute create file command     ${volume}/buck1/testfile     ${TEMP_DIR}/testfile
    Should contain     ${file.stdout}     http://httpfs:14000/webhdfs/v1/${volume}/buck1/testfile

Read file
    ${file} =       Execute curl command    ${volume}/buck1/testfile     OPEN    -L
    Should contain     ${file.stdout}     Hello world!

# Missing functionality, not working properly yet.
# List directory iteratively
    # ${list} =       Execute curl command    vol1          LISTSTATUS_BATCH&startAfter=buck1      ${EMPTY}
    # Should contain  ${list.stdout}     DirectoryListing    buck2
    # Should not contain          ${list.stdout}             buck1

Delete bucket
    ${bucket} =     Execute curl command    ${volume}/buck2          DELETE      -X DELETE
    Should contain  ${bucket.stdout}   true

Get status of bucket
    ${status} =     Execute curl command    ${volume}/buck1          GETFILESTATUS      ${EMPTY}
    Should contain  ${status.stdout}   FileStatus  DIRECTORY

Get status of file
    ${status} =     Execute curl command    ${volume}/buck1/testfile          GETFILESTATUS      ${EMPTY}
    Should contain  ${status.stdout}   FileStatus  FILE    13

List bucket
    ${list} =       Execute curl command    ${volume}/buck1          LISTSTATUS      ${EMPTY}
    Should contain  ${list.stdout}     FileStatus  testfile    FILE    13

List file
    ${list} =       Execute curl command    ${volume}/buck1/testfile          LISTSTATUS      ${EMPTY}
    Should contain  ${list.stdout}     FileStatus  FILE    13

Get content summary of directory
    ${summary} =    Execute curl command    ${volume}          GETCONTENTSUMMARY      ${EMPTY}
    Should contain  ${summary.stdout}  ContentSummary      "directoryCount":2      "fileCount":1

Get quota usage of directory
    ${usage} =      Execute curl command    ${volume}          GETQUOTAUSAGE      ${EMPTY}
    Should contain  ${usage.stdout}    QuotaUsage          "fileAndDirectoryCount":3

# Missing functionality, not working yet.
# Set permission of bucket
    # ${status} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json} =       Evaluate                json.loads('''${status.stdout}''')          json
    # ${permission} =     Set Variable     ${json["FileStatus"]["permission"]}
    # Execute curl command    vol1/buck1          SETPERMISSION&permission=666      -X PUT
    # ${status_after} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json_after} =       evaluate                json.loads('''${status_after.stdout}''')    json
    # ${permission_after} =   Set Variable     ${json_after["FileStatus"]["permission"]}
    # Should be equal As Integers     666   ${permission_after}

# Missing functionality, not working properly yet.
# Set replication factor of bucket
    # ${status} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json} =       Evaluate                json.loads('''${status.stdout}''')          json
    # ${factor} =     Set Variable     ${json["FileStatus"]["replication"]}
    # ${cmd} =        Execute curl command    vol1/buck1          SETREPLICATION&replication=1      -X PUT
    # Should contain  ${cmd.stdout}      true
    # ${status_after} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json_after} =       evaluate                json.loads('''${status_after.stdout}''')    json
    # ${factor_after} =   Set Variable     ${json_after["FileStatus"]["replication"]}
    # Should be equal As Integers     1   ${factor_after}

# Missing functionality, not working properly yet.
#Set access and modification time of bucket
    # ${status} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json} =       Evaluate                json.loads('''${status.stdout}''')          json
    # ${access} =     Set Variable     ${json["FileStatus"]["accessTime"]}
    # ${mod} =     Set Variable     ${json["FileStatus"]["modificationTime"]}
    # Execute curl command    vol1/buck1          SETTIMES&modificationtime=10&accesstime=10      -X PUT
    # ${status_after} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json_after} =       evaluate                json.loads('''${status_after.stdout}''')    json
    # ${access_after} =   Set Variable     ${json_after["FileStatus"]["accessTime"]}
    # ${mod_after} =   Set Variable     ${json_after["FileStatus"]["modificationTime"]}
    # Should be equal As Integers     10   ${access_after}
    # Should be equal As Integers     10   ${mod_after}

# Missing functionality, not working properly yet.
# Set owner of bucket
    # ${status} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json} =       Evaluate                json.loads('''${status.stdout}''')          json
    # ${owner} =     Set Variable     ${json["FileStatus"]["owner"]}
    # Execute curl command    vol1/buck1          SETOWNER&owner=hadoop      -X PUT
    # ${status_after} =     Execute curl command    vol1/buck1          GETFILESTATUS      ${EMPTY}
    # ${json_after} =       evaluate                json.loads('''${status_after.stdout}''')    json
    # ${owner_after} =   Set Variable     ${json_after["FileStatus"]["owner"]}
    # Should be equal     hadoop   ${owner_after}
