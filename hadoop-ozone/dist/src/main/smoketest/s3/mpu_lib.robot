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

Perform Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${result} =         Execute AWSS3APICli     create-multipart-upload --bucket ${bucket} --key ${key}
    ${upload_id} =      Execute and checkrc     echo '${result}' | jq -r '.UploadId'    0

    @{etags} =    Create List
    FOR    ${i}    ${file}    IN ENUMERATE    @{files}
        ${part} =    Evaluate    ${i} + 1
        ${result} =   Execute AWSS3APICli     upload-part --bucket ${bucket} --key ${key} --part-number ${part} --body ${file} --upload-id ${upload_id}
        ${etag} =     Execute                 echo '${result}' | jq -r '.ETag'
        Append To List    ${etags}    {ETag=${etag},PartNumber=${part}}
    END

    ${parts} =    Catenate    SEPARATOR=,    @{etags}
    Execute AWSS3APICli     complete-multipart-upload --bucket ${bucket} --key ${key} --upload-id ${upload_id} --multipart-upload 'Parts=[${parts}]'


Verify Multipart Upload
    [arguments]    ${bucket}    ${key}    @{files}

    ${random} =    Generate Ozone String

    Execute AWSS3APICli     get-object --bucket ${bucket} --key ${key} /tmp/verify${random}
    ${tmp} =    Catenate    @{files}
    Execute    cat ${tmp} > /tmp/original${random}
    Compare files    /tmp/original${random}    /tmp/verify${random}

