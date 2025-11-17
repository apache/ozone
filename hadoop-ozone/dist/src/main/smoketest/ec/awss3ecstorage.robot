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
Documentation       S3 gateway test with aws cli with STANDARD_IA storage class
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Resource            ../s3/mpu_lib.robot
Resource            ../ozone-lib/shell.robot
Test Timeout        5 minutes
Suite Setup         Setup EC Multipart Tests
Suite Teardown      Teardown EC Multipart Tests
Test Setup          Generate random prefix

*** Keywords ***
Setup EC Multipart Tests
    Setup s3 tests
    Create Random File KB    1023    /tmp/1mb

Teardown EC Multipart Tests
    Remove Files    /tmp/1mb

Count Datanodes In Service
    ${actual} =    Execute    ozone admin datanode list --node-state HEALTHY --operational-state IN_SERVICE --json | jq -r 'length'
    [return]       ${actual}

Has Enough Datanodes
    [arguments]    ${expected}
    ${actual} =    Count Datanodes In Service
    Should Be True    ${expected} <= ${actual}


*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Put Object with STANDARD_IA storage class
    Wait Until Keyword Succeeds      2min       10sec      Has Enough Datanodes    5

    ${file_checksum} =  Execute                    md5sum /tmp/1mb | awk '{print $1}'

    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/ecKey32 --body /tmp/1mb --storage-class STANDARD_IA
    ${eTag} =           Execute                    echo '${result}' | jq -r '.ETag'
                        Should Be Equal            ${eTag}           \"${file_checksum}\"
                        Verify Key EC Replication Config    /s3v/${BUCKET}/${PREFIX}/ecKey32    RS    3    2    1048576

    Wait Until Keyword Succeeds      2min       10sec      Has Enough Datanodes    9

    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/ecKey63 --body /tmp/1mb --storage-class STANDARD_IA --metadata="storage-config=rs-6-3-1024k"
    ${eTag} =           Execute                    echo '${result}' | jq -r '.ETag'
                        Should Be Equal            ${eTag}           \"${file_checksum}\"
                        Verify Key EC Replication Config    /s3v/${BUCKET}/${PREFIX}/ecKey63    RS    6    3    1048576

Test multipart upload with STANDARD_IA storage
    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/ecmultipartKey32     0     --storage-class STANDARD_IA
    ${eTag1} =          Upload MPU part    ${BUCKET}    ${PREFIX}/ecmultipartKey32    ${uploadID}    1    /tmp/1mb
    ${result} =         Execute AWSS3APICli   list-parts --bucket ${BUCKET} --key ${PREFIX}/ecmultipartKey32 --upload-id ${uploadID}
    ${part1} =          Execute               echo '${result}' | jq -r '.Parts[0].ETag'
                        Should Be equal       ${part1}    ${eTag1}
                        Should contain        ${result}    STANDARD_IA
                        Complete MPU    ${BUCKET}    ${PREFIX}/ecmultipartKey32    ${uploadID}    {ETag=${eTag1},PartNumber=1}
                        Verify Key EC Replication Config    /s3v/${BUCKET}/${PREFIX}/ecmultipartKey32    RS    3    2    1048576

    ${uploadID} =       Initiate MPU    ${BUCKET}    ${PREFIX}/ecmultipartKey63     0     --storage-class STANDARD_IA --metadata="storage-config=rs-6-3-1024k"
    ${eTag1} =          Upload MPU part    ${BUCKET}    ${PREFIX}/ecmultipartKey63    ${uploadID}    1    /tmp/1mb
    ${result} =         Execute AWSS3APICli   list-parts --bucket ${BUCKET} --key ${PREFIX}/ecmultipartKey63 --upload-id ${uploadID}
    ${part1} =          Execute               echo '${result}' | jq -r '.Parts[0].ETag'
                        Should Be equal       ${part1}    ${eTag1}
                        Should contain        ${result}    STANDARD_IA
                        Complete MPU    ${BUCKET}    ${PREFIX}/ecmultipartKey63    ${uploadID}    {ETag=${eTag1},PartNumber=1}
                        Verify Key EC Replication Config    /s3v/${BUCKET}/${PREFIX}/ecmultipartKey63    RS    6    3    1048576

Copy Object change storage class to STANDARD_IA
    ${file_checksum} =  Execute                    md5sum /tmp/1mb | awk '{print $1}'
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/copyobject/Key1 --body /tmp/1mb
    ${eTag} =           Execute                    echo '${result}' | jq -r '.ETag'
                        Should Be Equal            ${eTag}           \"${file_checksum}\"

     ${result} =         Execute AWSS3APICli        copy-object --storage-class STANDARD_IA --bucket ${BUCKET} --key ${PREFIX}/copyobject/Key1 --copy-source ${BUCKET}/${PREFIX}/copyobject/Key1
                         Should contain             ${result}        ETag
     ${eTag} =           Execute                    echo '${result}' | jq -r '.CopyObjectResult.ETag'
                         Should Be Equal            ${eTag}           \"${file_checksum}\"

     ${result} =         Execute AWSS3APICli        copy-object --storage-class STANDARD_IA --metadata="storage-config=rs-6-3-1024k" --bucket ${BUCKET} --key ${PREFIX}/copyobject/Key1 --copy-source ${BUCKET}/${PREFIX}/copyobject/Key1
                         Should contain             ${result}        ETag
     ${eTag} =           Execute                    echo '${result}' | jq -r '.CopyObjectResult.ETag'
                         Should Be Equal            ${eTag}           \"${file_checksum}\"
                         ## TODO: Verify Key EC Replication Config when we support changing storage class
