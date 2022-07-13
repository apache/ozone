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
    ${result} =         Execute AWSS3APICli and checkrc    put-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1 --body /tmp/testfile   0

    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/putobject/key=value/f1   0

Head object in unexisting bucket
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET}-non-exitst --key ${PREFIX}/putobject/key=value/f1   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found

Head unexisting key
    ${result} =         Execute AWSS3APICli and checkrc    head-object --bucket ${BUCKET} --key ${PREFIX}/non-exist   255
                        Should contain          ${result}    404
                        Should contain          ${result}    Not Found