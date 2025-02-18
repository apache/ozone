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
Put object s3a simulation
                        Execute                    echo "Randomtext" > /tmp/testfile
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt    255
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/word.txt/
                        Should Not Match Regexp    ${result}   "Key".*word.txt

    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt._COPYING_    255
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/word.txt._COPYING_/
                        Should Not Match Regexp    ${result}   "Key".*word.txt._COPYING_

    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/word.txt._COPYING_ --body /tmp/testfile
                        Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt._COPYING_    0
                        Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt    255
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/word.txt/
                        Should Not Match Regexp    ${result}   "Key".*word.txt._COPYING_

    ${result} =         Execute AWSS3ApiCli        copy-object --bucket ${BUCKET} --key ${PREFIX}/word.txt --copy-source ${BUCKET}/${PREFIX}/word.txt._COPYING_
                        Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt    0
                        Execute AWSS3APICli        delete-object --bucket ${BUCKET} --key ${PREFIX}/word.txt._COPYING_
                        Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/word.txt._COPYING_    255
