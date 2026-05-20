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
${ENDPOINT_URL}       http://s3g:9878
${OZONE_TEST}         true
${BUCKET}             generated

*** Test Cases ***

Head existing object
                        Execute                            echo "Randomtext" > /tmp/testfile
    ${result} =         Execute AWSS3APICli and checkrc    put-object --bucket ${BUCKET} --key ${PREFIX}/headobject/key=value/f1 --body /tmp/testfile   0

    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/headobject/key=value/f1   0
                        Should Contain    ${result}          "StorageClass":
    ${result} =         Execute AWSS3APICli and checkrc    delete-object --bucket ${BUCKET} --key ${PREFIX}/headobject/key=value/f1   0

Head object in non existing bucket
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET}-non-existent --key ${PREFIX}/headobject/key=value/f1   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found

Head object where path is a directory
    Pass Execution If   '${BUCKET_LAYOUT}' == 'FILE_SYSTEM_OPTIMIZED'    does not apply to FSO buckets
    ${result} =         Execute AWSS3APICli and checkrc    put-object --bucket ${BUCKET} --key ${PREFIX}/headobject/keyvalue/f1 --body /tmp/testfile   0
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/headobject/keyvalue/   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found

Head directory objects
    Pass Execution If   '${BUCKET_LAYOUT}' == 'FILE_SYSTEM_OPTIMIZED'    does not apply to FSO buckets
    ${result} =         Execute AWSS3APICli and checkrc    put-object --bucket ${BUCKET} --key ${PREFIX}/mydir/ --body /tmp/testfile   0
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/mydir   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/mydir/   0

Head non existing key
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/non-existent   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found

Check Bucket Ownership Verification
    Execute                                                  echo "Randomtext" > /tmp/testfile
    ${result} =  Execute AWSS3APICli and checkrc             put-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1 --body /tmp/testfile   0

    ${result} =  Execute AWSS3APICli and checkrc             head-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1 --expected-bucket-owner wrong-owner  255
                 Should contain                              ${result}  403

    ${correct_owner} =    Get bucket owner                   ${BUCKET}
    Execute AWSS3APICli using bucket ownership verification  head-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1    ${correct_owner}
