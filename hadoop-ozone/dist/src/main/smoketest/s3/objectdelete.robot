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
${BUCKET}             generated

*** Test Cases ***
Delete file with s3api
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapi/key=value/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/deletetestapi/key=value/
                        Should contain             ${result}         "${PREFIX}/deletetestapi/key=value/f1"
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapi/key=value/f1
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/deletetestapi/key=value/
                        Should not contain         ${result}         "${PREFIX}/deletetestapi/key=value/f1"

Delete file with s3api, file doesn't exist
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/
                        Should not contain         ${result}         thereisnosuchfile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key thereisnosuchfile
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/
                        Should not contain         ${result}         thereisnosuchfile

Delete dir with s3api
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3Cli           cp /tmp/testfile s3://${BUCKET}/${PREFIX}/deletetestapidir/key=value/f1
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/deletetestapidir/key=value/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapidir/key=value/
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/deletetestapidir/key=value/
                        Should contain             ${result}         f1
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapidir/key=value/f1


Delete file with s3api, file doesn't exist, prefix of a real file
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3Cli           cp /tmp/testfile s3://${BUCKET}/${PREFIX}/deletetestapiprefix/key=value/filefile
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/deletetestapiprefix/key=value/
                        Should contain             ${result}         filefile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapiprefix/key=value/file
    ${result} =         Execute AWSS3Cli           ls s3://${BUCKET}/${PREFIX}/deletetestapiprefix/key=value/
                        Should contain             ${result}         filefile
    ${result} =         Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/deletetestapiprefix/key=value/filefile



Delete file with s3api, bucket doesn't exist
    ${result} =         Execute AWSS3APICli and checkrc   delete-object --bucket ${BUCKET}-nosuchbucket --key f1      255
                        Should contain                    ${result}         NoSuchBucket

Check Bucket Ownership Verification
    Execute             echo "Randomtext" > /tmp/testfile
    ${result} =         Execute AWSS3APICli and checkrc                 put-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1 --body /tmp/testfile   0
    ${correct_owner} =    Get bucket owner                              ${BUCKET}
    # delete single object
    Execute AWSS3APICli with bucket owner check                         delete-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1  ${correct_owner}

    # delete multiple objects
    Execute                                                             echo "Randomtext" > /tmp/testfile1
    Execute                                                             echo "Randomtext" > /tmp/testfile2
    Execute AWSS3APICli and checkrc                                     put-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/multi1 --body /tmp/testfile1   0
    Execute AWSS3APICli and checkrc                                     put-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/multi2 --body /tmp/testfile2   0

    ${delete_json} =    Set Variable                                    {"Objects": [{"Key": "${PREFIX}/bucketownercondition/key=value/multi1"}, {"Key": "${PREFIX}/bucketownercondition/key=value/multi2"}]}
    ${delete_file} =    Set Variable                                    /tmp/delete_objects.json
    Execute                                                             echo '${delete_json}' > ${delete_file}
    Execute AWSS3APICli with bucket owner check                         delete-objects --bucket ${BUCKET} --delete file://${delete_file}  ${correct_owner}
