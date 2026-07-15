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
Documentation       Keywords for Multipart Upload
Library             OperatingSystem
Library             String
Resource            commonawslib.robot

*** Keywords ***

Initiate MPU
    [arguments]    ${bucket}    ${key}    ${expected_rc}=0    ${opts}=${EMPTY}

    ${result} =    Execute AWSS3APICli and checkrc    create-multipart-upload --bucket ${bucket} --key ${key} ${opts}    ${expected_rc}
    IF    '${expected_rc}' == '0'
        Should contain          ${result}    ${bucket}
        Should contain          ${result}    ${key}
        ${upload_id} =      Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0
        RETURN    ${upload_id}
    ELSE
        RETURN    ${result}
    END


Upload MPU part
    [arguments]    ${bucket}    ${key}    ${upload_id}    ${part}    ${file}    ${expected_rc}=0

    ${result} =    Execute AWSS3APICli and checkrc    upload-part --bucket ${bucket} --key ${key} --part-number ${part} --body ${file} --upload-id ${upload_id}    ${expected_rc}
    IF    '${expected_rc}' == '0'
        Should contain    ${result}    ETag
        ${etag} =      Execute    echo '${result}' | jq -r '.ETag'
        ${etag} =      Replace String    ${etag}    \"    ${EMPTY}
        ${md5sum} =    Execute    md5sum ${file} | awk '{print $1}'
        Should Be Equal As Strings    ${etag}    ${md5sum}
        RETURN    ${etag}
    ELSE
        RETURN    ${result}
    END


Complete MPU
    [arguments]    ${bucket}    ${key}    ${upload_id}    ${parts}    ${expected_rc}=0

    ${result} =    Execute AWSS3APICli and checkrc    complete-multipart-upload --bucket ${bucket} --key ${key} --upload-id ${upload_id} --multipart-upload 'Parts=[${parts}]'    ${expected_rc}
    IF    '${expected_rc}' == '0'
        Should contain    ${result}    ${bucket}
        Should contain    ${result}    ${key}
        Should contain    ${result}    ETag
        ${etag} =    Execute    echo '${result}' | jq -r '.ETag'
        RETURN    ${etag}
    ELSE
        RETURN    ${result}
    END


Abort MPU
    [arguments]    ${bucket}    ${key}    ${upload_id}    ${expected_rc}=0

    ${result} =    Execute AWSS3APICli and checkrc    abort-multipart-upload --bucket ${bucket} --key ${key} --upload-id ${upload_id}    ${expected_rc}


Upload MPU parts
    [arguments]    ${bucket}    ${key}    ${upload_id}    @{files}

    @{etags} =    Create List
    FOR    ${i}    ${file}    IN ENUMERATE    @{files}
        ${part} =    Evaluate    ${i} + 1
        ${etag} =    Upload MPU part    ${bucket}    ${key}    ${upload_id}    ${part}    ${file}
        Append To List    ${etags}    {ETag=${etag},PartNumber=${part}}
    END
    ${parts} =    Catenate    SEPARATOR=,    @{etags}

    RETURN    ${parts}


Perform Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${upload_id} =      Initiate MPU    ${bucket}    ${key}
    ${parts} =          Upload MPU parts    ${bucket}    ${key}    ${upload_id}    @{files}
    Complete MPU    ${bucket}    ${key}    ${upload_id}    ${parts}


Verify Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${random} =    Generate Ozone String

    Execute AWSS3APICli     get-object --bucket ${bucket} --key ${key} /tmp/verify${random}
    ${tmp} =    Catenate    @{files}
    Execute    cat ${tmp} > /tmp/original${random}
    Compare files    /tmp/original${random}    /tmp/verify${random}

