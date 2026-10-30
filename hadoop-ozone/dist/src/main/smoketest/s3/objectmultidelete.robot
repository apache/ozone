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
Library             ./presigned_url_helper.py
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${ENDPOINT_URL}       http://s3g:9878
${BUCKET}             generated

*** Test Cases ***

Delete file with multi delete
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/key=value/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/key=value/f2 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/key=value/f3 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/multidelete/key=value/
                        Should contain             ${result}         ${PREFIX}/multidelete/key=value/f1
                        Should contain             ${result}         ${PREFIX}/multidelete/key=value/f2
                        Should contain             ${result}         ${PREFIX}/multidelete/key=value/f3
                        Should contain             ${result}         STANDARD
                        Should not contain         ${result}         REDUCED_REDUNDANCY
    ${result} =         Execute AWSS3APICli        delete-objects --bucket ${BUCKET} --delete 'Objects=[{Key=${PREFIX}/multidelete/key=value/f1},{Key=${PREFIX}/multidelete/key=value/f2},{Key=${PREFIX}/multidelete/key=value/f4}]'
                        Should not contain         ${result}         Error
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/multidelete/key=value/
                        Should not contain         ${result}         ${PREFIX}/multidelete/key=value/f1
                        Should not contain         ${result}         ${PREFIX}/multidelete/key=value/f2
                        Should contain             ${result}         ${PREFIX}/multidelete/key=value/f3
                        Should contain             ${result}         STANDARD
                        Should not contain         ${result}         REDUCED_REDUNDANCY

Multi delete rejects a key with mixed content
    [Documentation]    A Key holding text, a child element and more text is malformed. It must be rejected,
    ...                not read as only the text after the child element, which would delete another key.
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}-mixed --body /tmp/testfile
    ${access_key} =     Execute                    aws configure get aws_access_key_id
    ${secret_key} =     Execute                    aws configure get aws_secret_access_key
    ${url} =            Generate Presigned Delete Objects Url    ${access_key}    ${secret_key}    ${BUCKET}    us-east-1    3600    ${ENDPOINT_URL}
                        Create File                /tmp/${PREFIX}-mixed.xml    <Delete><Object><Key>dir/<a/>${PREFIX}-mixed</Key></Object></Delete>
    ${result} =         Execute                    curl -sS -i -X POST -H "Content-Type: application/xml" --data-binary @/tmp/${PREFIX}-mixed.xml "${url}"
                        Should Contain             ${result}         InvalidRequest
                        Should Contain             ${result}         Mixed content is not allowed
    ${result} =         Execute AWSS3ApiCli        head-object --bucket ${BUCKET} --key ${PREFIX}-mixed
    [Teardown]          Remove File                /tmp/${PREFIX}-mixed.xml
