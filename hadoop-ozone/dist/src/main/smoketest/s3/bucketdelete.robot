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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${BUCKET}             generated
${ENDPOINT_URL}       http://s3g:9878

*** Keywords ***
Create bucket to be deleted
    ${bucket} =    Run Keyword if    '${BUCKET}' == 'link'    Create link    to-be-deleted
    ...            ELSE              Run Keyword              Create bucket
    [return]       ${bucket}

*** Test Cases ***

Delete existing bucket
    ${bucket} =                Create bucket to be deleted
    Execute AWSS3APICli        delete-bucket --bucket ${bucket}

Delete non-existent bucket
    [tags]    no-bucket-type
    ${randStr} =   Generate Ozone String
    ${result} =    Execute AWSS3APICli and checkrc    delete-bucket --bucket nosuchbucket-${randStr}    255
                   Should contain                     ${result}                              NoSuchBucket

Delete bucket with incomplete multipart uploads
    [tags]    no-bucket-type
    ${bucket} =                Create bucket

    # initiate incomplete multipart uploads (multipart upload is initiated but not completed/aborted)
    ${initiate_result} =       Execute AWSS3APICli     create-multipart-upload --bucket ${bucket} --key incomplete-multipartkey
    ${uploadID} =              Execute                 echo '${initiate_result}' | jq -r '.UploadId'
                               Should contain          ${initiate_result}    ${bucket}
                               Should contain          ${initiate_result}    incomplete-multipartkey
                               Should contain          ${initiate_result}    UploadId

    # bucket deletion should fail since there is still incomplete multipart upload
    ${delete_fail_result} =    Execute AWSS3APICli and checkrc    delete-bucket --bucket ${bucket}    255
                               Should contain                     ${delete_fail_result}               BucketNotEmpty

    # after aborting the multipart upload, the bucket deletion should succeed
    ${abort_result} =          Execute AWSS3APICli and checkrc    abort-multipart-upload --bucket ${bucket} --key incomplete-multipartkey --upload-id ${uploadID}   0
    ${delete_result} =         Execute AWSS3APICli and checkrc    delete-bucket --bucket ${bucket}    0