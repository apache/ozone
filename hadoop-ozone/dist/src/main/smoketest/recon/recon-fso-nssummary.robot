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
Documentation       Smoke test for Recon Namespace Summary Endpoint for FSO buckets.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}             http://recon:9888
${API_ENDPOINT_URL}         ${ENDPOINT_URL}/api/v1
${ADMIN_NAMESPACE_URL}      ${API_ENDPOINT_URL}/namespace
${SUMMARY_URL}              ${ADMIN_NAMESPACE_URL}/summary
${DISK_USAGE_URL}           ${ADMIN_NAMESPACE_URL}/du
${QUOTA_USAGE_URL}          ${ADMIN_NAMESPACE_URL}/quota
${FILE_SIZE_DIST_URL}       ${ADMIN_NAMESPACE_URL}/dist
${VOLUME}
${BUCKET}

*** Keywords ***
Create volume
    ${random} =     Generate Random String  5  [LOWER]
                    Set Suite Variable     ${VOLUME}    vol-${random}
    ${result} =     Execute             ozone sh volume create /${VOLUME}
                    Should not contain  ${result}       Failed

Create bucket
    ${random} =     Generate Random String  5  [LOWER]
                    Set Suite Variable     ${BUCKET}    buc-${random}
    ${result} =     Execute             ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${VOLUME}/${BUCKET}
                    Should not contain  ${result}       Failed

Create keys
    ${result} =     Execute             ozone sh key put /${VOLUME}/${BUCKET}/file1 README.md
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh key put /${VOLUME}/${BUCKET}/dir1/dir2/file2 HISTORY.md
                    Should not contain  ${result}       Failed
Kinit as non admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     scm     scm.keytab

Kinit as ozone admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Kinit as recon admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser2           testuser2.keytab

Check http return code
    [Arguments]         ${url}          ${expected_code}
    ${result} =         Execute                             curl --negotiate -u : --write-out '\%{http_code}\n' --silent --show-error --output /dev/null ${url}
                        IF  '${SECURITY_ENABLED}' == 'true'
                            Should contain      ${result}       ${expected_code}
                        ELSE
                            # All access should succeed without security.
                            Should contain      ${result}       200
                        END

Check Access
    [Arguments]         ${url}
    Execute    kdestroy
    Check http return code      ${url}       401

    kinit as non admin
    Check http return code      ${url}       403

    kinit as ozone admin
    Check http return code      ${url}       200

    kinit as recon admin
    Check http return code      ${url}       200

Test Summary                            
    [Arguments]         ${url}        ${expected}
           ${result} =         Execute                              curl --negotiate -u : -LSs ${url}
                               Should contain      ${result}       \"status\":\"OK\"
                               Should contain      ${result}       ${expected}

Wait For Summary
    [Arguments]         ${url}        ${expected}
    Wait Until Keyword Succeeds     90sec      10sec        Test Summary      ${url}        ${expected}

*** Test Cases ***

Check volume creation
    Execute    kdestroy
    Kinit as ozone admin
    Create volume

Check bucket creation
    Create bucket

Check keys creation
    Create keys

Check Summary api access
    Check access      ${SUMMARY_URL}?path=/

Check Disk Usage api access
    Check access       ${DISK_USAGE_URL}?path=/

Check Quota Usage api access
    Check access       ${QUOTA_USAGE_URL}?path=/

Check File Size Distribution api access
    Check access       ${FILE_SIZE_DIST_URL}?path=/

Check Recon Namespace Summary Root
    Wait For Summary      ${SUMMARY_URL}?path=/       ROOT

Check Recon Namespace Summary Volume
    Wait For Summary      ${SUMMARY_URL}?path=/${VOLUME}   VOLUME

Check Recon Namespace Summary Bucket
    Wait For Summary      ${SUMMARY_URL}?path=/${VOLUME}/${BUCKET}    BUCKET

Check Recon Namespace Summary Key
    Wait For Summary      ${SUMMARY_URL}?path=/${VOLUME}/${BUCKET}/file1   KEY

Check Recon Namespace Summary Directory
    Wait For Summary      ${SUMMARY_URL}?path=/${VOLUME}/${BUCKET}/dir1/dir2   DIRECTORY

Check Recon Namespace Disk Usage
    Wait For Summary      ${DISK_USAGE_URL}?path=/${VOLUME}/${BUCKET}&files=true&replica=true     \"sizeWithReplica\"

Check Recon Namespace Volume Quota Usage
    Wait For Summary      ${QUOTA_USAGE_URL}?path=/${VOLUME}             \"used\"

Check Recon Namespace Bucket Quota Usage
    Wait For Summary      ${QUOTA_USAGE_URL}?path=/${VOLUME}/${BUCKET}   \"used\"

Check Recon Namespace File Size Distribution Root
    Wait For Summary      ${FILE_SIZE_DIST_URL}?path=/                   \"dist\"
