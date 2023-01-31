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
Default Tags        no-bucket-type

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Create new bucket
    Create bucket

Create bucket which already exists
    ${bucket} =                 Create bucket
    Create bucket with name     ${bucket}

Create bucket with invalid bucket name
    ${randStr} =        Generate Ozone String
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket invalid_bucket_${randStr}   255
                        Should contain              ${result}         InvalidBucketName
Create new bucket and check no group ACL
    ${bucket} =         Create bucket
    ${acl} =            Execute     ozone sh bucket getacl s3v/${bucket}
    ${group} =          Get Regexp Matches   ${acl}     "GROUP"
    IF      '${group}' is not '[]'
        ${json} =           Evaluate    json.loads('''${acl}''')    json
        # make sure this check is for group acl
        Should contain      ${json}[1][type]       GROUP
        Should contain      ${json}[1][aclList]    NONE
    END