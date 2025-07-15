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
# limitations under the License

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

Put object tagging
# Create an object and call put-object-tagging
                        Execute                    echo "Randomtext" > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/putobject/key=value/
                        Should contain             ${result}         f1

    ${result} =         Execute AWSS3ApiCli        put-object-tagging --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'
    ${result} =         Execute AWSS3APICli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 /tmp/testfile2.result
                        Should contain             ${result}   TagCount
    ${tagCount} =       Execute and checkrc        echo '${result}' | jq -r '.TagCount'    0
                        Should Be Equal            ${tagCount}    1

# Calling put-object-tagging again to overwrite the existing tags
    ${result} =         Execute AWSS3ApiCli        put-object-tagging --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --tagging '{"TagSet": [{ "Key": "tag-key2", "Value": "tag-value2" },{ "Key": "tag-key3", "Value": "tag-value3" }]}'
    ${result} =         Execute AWSS3APICli        get-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 /tmp/testfile2.result
                        Should contain             ${result}   TagCount
    ${tagCount} =       Execute and checkrc        echo '${result}' | jq -r '.TagCount'    0
                        Should Be Equal            ${tagCount}    2

# Calling put-object-tagging on non-existent key
    ${result} =         Execute AWSS3APICli and checkrc    put-object-tagging --bucket ${BUCKET} --key ${PREFIX}/nonexistent --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'   255
                        Should contain             ${result}    NoSuchKey

#This test depends on the previous test case. Can't be executes alone
Get object tagging

    ${result} =         Execute AWSS3ApiCli        get-object-tagging --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1
                        Should contain             ${result}   TagSet
    ${tagCount} =       Execute and checkrc        echo '${result}' | jq '.TagSet | length'    0
                        Should Be Equal            ${tagCount}    2


#This test depends on the previous test case. Can't be executes alone
Delete object tagging

     ${result} =         Execute AWSS3ApiCli        delete-object-tagging --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1
     ${result} =         Execute AWSS3ApiCli        get-object-tagging --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1
                         Should contain             ${result}   TagSet
     ${tagCount} =       Execute and checkrc        echo '${result}' | jq '.TagSet | length'    0
                         Should Be Equal            ${tagCount}    0

Check Bucket Ownership Verification
    #Create object
    Execute                                                         echo "Randomtext" > /tmp/testfile
    Execute AWSS3APICli                                             put-object --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1 --body /tmp/testfile
    ${correct_owner} =    Get bucket owner    ${BUCKET}

    #Create tagging
    Execute AWSS3APICli with bucket owner check                     put-object-tagging --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1 --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'  ${correct_owner}

    #Get object tagging
    Execute AWSS3APICli with bucket owner check                     get-object-tagging --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1  ${correct_owner}

    #Delete object tagging
    Execute AWSS3APICli with bucket owner check                     delete-object-tagging --bucket ${BUCKET} --key ${PREFIX}/bucketownercondition/key=value/f1  ${correct_owner}
