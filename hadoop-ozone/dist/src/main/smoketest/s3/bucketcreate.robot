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
Test Tags           no-bucket-type

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Create new bucket
    Create bucket

Create bucket with maximum valid name length
  ${randStr} =        Generate Random String     62    [LOWER]
  ${bucket} =         Set Variable               b${randStr}
                      Create bucket with name    ${bucket}

Create bucket which already exists
    ${bucket} =         Create bucket
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket ${bucket}   255
                        Should contain          ${result}           BucketAlreadyOwnedByYou

Create bucket with invalid bucket name
    ${randStr} =        Generate Ozone String
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket invalid_bucket_${randStr}   255
                        Should contain          ${result}           InvalidBucketName

Create bucket with name too short
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket ab   255
                        Should contain          ${result}           InvalidBucketName

Create bucket with name too long
    ${bucket} =         Evaluate    'a' * 64
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket ${bucket}   255
                        Should contain          ${result}           InvalidBucketName

Create bucket with all uppercase characters in bucket name
    ${randStr} =        Generate Random String     8    [UPPER]
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket BUCKET${randStr}   255
                        Should contain          ${result}           InvalidBucketName

Create bucket with mixed uppercase characters in bucket name
    ${randStr} =        Generate Random String     8    [LOWER]
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket BuCkEt-${randStr}   255
                        Should contain          ${result}           InvalidBucketName


Create bucket validate names must begin and end with a letter or number.
    ${randStr} =        Generate Random String     8    [LOWER]
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket bucket-${randStr}-   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not contain two adjacent periods.
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket test..bucket   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not be formatted as an IP address (for example, 192.168.5.4).
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket 192.168.5.4   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not contain leading dash
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket=-test   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not contain leading period
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket .test   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not contain trailing period
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket test.   255
                        Should contain          ${result}           InvalidBucketName

Create bucket validate names must not contain all numeric
    ${randStr} =        Generate Random String     8    [NUMBERS]
    ${result} =         Execute AWSS3APICli and checkrc         create-bucket --bucket ${randStr}   255
                        Should contain          ${result}           InvalidBucketName

Create new bucket and check default group ACL
    [tags]    aws-skip
    ${bucket} =         Create bucket
    ${acl} =            Execute     ozone sh bucket getacl s3v/${bucket}
    ${group} =          Get Regexp Matches   ${acl}     "GROUP"
    IF      '${group}' != '[]'
        ${json} =           Evaluate    json.loads('''${acl}''')    json
        # make sure this check is for group acl
        Should contain      ${json}[1][type]       GROUP
        Should contain      ${json}[1][aclList]    READ
        Should contain      ${json}[1][aclList]    LIST
    END

Test buckets named like web endpoints
    [tags]    aws-skip
    ${path} =    Create Random File KB    64

    FOR  ${name}   IN    conf    jmx    logs    logstream    prof    prom    secret    stacks    static
        Create bucket with name    ${name}
        Put object to bucket    bucket=${name}    key=testkey    path=${path}
    END

Check bucket ownership verification
    [tags]    bucket-ownership-verification
    ${bucket} =           Create bucket
    ${correct_owner} =    Get bucket owner    ${bucket}

    Execute AWSS3APICli with bucket owner check        put-bucket-acl --bucket ${bucket} --grant-full-control id=${correct_owner}  ${correct_owner}
    Execute AWSS3APICli with bucket owner check        get-bucket-acl --bucket ${bucket}    ${correct_owner}
