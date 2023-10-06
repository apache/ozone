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
Documentation       Smoke test to start cluster with docker-compose environments.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../lib/fs.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}    http://s3g:9878
${SCM}             scm
${TMP_MOUNT}       tmp
${TMP_DIR}         tmp
${SCHEME}          ofs

*** Keywords ***
Setup volume names
    ${random}            Generate Random String  2   [NUMBERS]
    Set Suite Variable   ${volume1}            fstest${random}
    Set Suite Variable   ${volume2}            fstest2${random}
    Set Suite Variable   ${volume3}            fstest3${random}
    Set Suite Variable   ${volume4}            fstest4${random}

Format ofs TMPMOUNT
    [arguments]    ${volume}  ${path}=${EMPTY}    ${om}=${OM_SERVICE_ID}

    ${om_with_trailing} =     Run Keyword If    '${om}' != '${EMPTY}'      Ensure Trailing   /    ${om}
    ...                       ELSE              Set Variable    ${EMPTY}

    ${path_with_leading} =    Run Keyword If    '${path}' != '${EMPTY}'    Ensure Leading    /    ${path}
    ...                       ELSE              Set Variable    ${EMPTY}

    [return]       ofs://${om_with_trailing}${volume}/${path_with_leading}

*** Test Cases ***
Create volume bucket with wrong credentials
    Execute             kdestroy
    ${rc}               ${output} =          Run And Return Rc And Output       ozone sh volume create o3://om/fstest
    Should contain      ${output}       Client cannot authenticate via

Create volume with non-admin user
    Run Keyword         Kinit test user     testuser2     testuser2.keytab
    ${rc}               ${output} =          Run And Return Rc And Output       ozone sh volume create o3://om/fstest
    Should contain      ${output}       doesn't have CREATE permission to access volume

Create bucket with non-admin owner(testuser2)
    Run Keyword   Kinit test user     testuser     testuser.keytab
    Run Keyword   Setup volume names
    Execute       ozone sh volume create o3://om/${volume4} -u testuser2
    Run Keyword   Kinit test user     testuser2    testuser2.keytab
    ${result} =   Execute     ozone sh bucket create o3://om/${volume4}/bucket1 --layout FILE_SYSTEM_OPTIMIZED
                  Should not contain  ${result}       PERMISSION_DENIED
    ${result} =   Execute     ozone sh key put ${volume4}/bucket1/key1 /opt/hadoop/NOTICE.txt
                  Should not contain  ${result}       PERMISSION_DENIED
    ${result} =   Execute     ozone sh key list ${volume4}/bucket1
                  Should not contain  ${result}       PERMISSION_DENIED
    ${result} =   Execute     ozone sh key delete ${volume4}/bucket1/key1
                  Should not contain  ${result}       PERMISSION_DENIED
    ${result} =   Execute     ozone sh bucket delete -r --yes ${volume4}/bucket1
                  Should not contain  ${result}       PERMISSION_DENIED

Create volume bucket with credentials
                        # Authenticate testuser
    Run Keyword         Kinit test user     testuser     testuser.keytab
    Run Keyword         Setup volume names
    Execute             ozone sh volume create o3://om/${volume1}
    Execute             ozone sh volume create o3://om/${volume2}
    Execute             ozone sh bucket create o3://om/${volume1}/bucket1 --layout FILE_SYSTEM_OPTIMIZED
    Execute             ozone sh bucket create o3://om/${volume1}/bucket2 --layout FILE_SYSTEM_OPTIMIZED
    Execute             ozone sh bucket create o3://om/${volume2}/bucket3 --layout FILE_SYSTEM_OPTIMIZED

Check volume from ozonefs
    ${result} =         Execute          ozone fs -ls o3fs://bucket1.${volume1}/

Test tmp mount for shared ofs tmp dir
   ${result} =      Execute And Ignore Error    ozone getconf confKey ozone.om.enable.ofs.shared.tmp.dir
   ${contains} =    Evaluate        "true" in """${result}"""
   IF   ${contains} == ${True}
        Run Keyword   Kinit test user     testuser     testuser.keytab
        Execute       ozone sh volume create /${TMP_MOUNT} -u testuser
        Execute       ozone sh bucket create /${TMP_MOUNT}/${TMP_DIR} -u testuser
        Execute       ozone sh volume addacl /${TMP_MOUNT} -a user:testuser:a,user:testuser2:rw
        Execute       ozone sh bucket addacl /${TMP_MOUNT}/${TMP_DIR} -a user:testuser:a,user:testuser2:rwlc

        ${tmpdirmount} =        Format ofs TMPMOUNT     ${TMP_MOUNT}
        ${result} =    Execute               ozone fs -put ./NOTICE.txt ${tmpdirmount}
                   Should Be Empty       ${result}
        Run Keyword   Kinit test user     testuser2     testuser2.keytab
        ${result} =    Execute               ozone fs -put ./LICENSE.txt ${tmpdirmount}
                   Should Be Empty       ${result}

        ${result} =    Execute               ozone fs -ls ${tmpdirmount}
                   Should contain        ${result}         NOTICE.txt
                   Should contain        ${result}         LICENSE.txt


        ${result} =     Execute And Ignore Error         ozone fs -rm -skipTrash ${tmpdirmount}/NOTICE.txt
                    Should contain      ${result}    error
        ${result} =    Execute               ozone fs -rm -skipTrash ${tmpdirmount}/LICENSE.txt
                   Should contain        ${result}         Deleted

        Run Keyword   Kinit test user     testuser     testuser.keytab
        ${result} =    Execute               ozone fs -rm -skipTrash ${tmpdirmount}/NOTICE.txt
                   Should contain        ${result}         Deleted

        Execute       ozone fs -rm -r -skipTrash ${tmpdirmount}
        Execute       ozone sh volume delete /${TMP_MOUNT}
   END

