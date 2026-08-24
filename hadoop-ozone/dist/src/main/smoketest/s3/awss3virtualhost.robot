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
Documentation       S3 gateway test with aws cli using virtual host style address
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            ./commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests
Default Tags        virtual-host

*** Variables ***
${ENDPOINT_URL}                http://s3g.internal:9878
${BUCKET}                      bucket1
${OZONE_S3_ADDRESS_STYLE}      virtual

*** Test Cases ***

File upload and directory list with virtual style addressing
                        Create bucket with name     ${BUCKET}
                        Execute                   date > /tmp/testfile
    ${result} =         Execute AWSS3Cli          cp /tmp/testfile s3://${BUCKET} --debug
                        Should contain            ${result}         url=http://bucket1.s3g.internal:9878/
                        Should contain            ${result}         upload
    ${result} =         Execute AWSS3Cli          cp /tmp/testfile s3://${BUCKET}/dir1/dir2/file --debug
                        Should contain            ${result}         url=http://bucket1.s3g.internal:9878/dir1/dir2/file
                        Should contain            ${result}         upload
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET} --debug
                        Should contain            ${result}         url=http://bucket1.s3g.internal:9878/
                        Should contain            ${result}         testfile
                        Should contain            ${result}         dir1
                        Should not contain        ${result}         dir2
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET}/dir1/ --debug
                        Should contain            ${result}         url=http://bucket1.s3g.internal:9878/
                        Should contain            ${result}         prefix=dir1
                        Should not contain        ${result}         testfile
                        Should contain            ${result}         dir2/
    ${result} =         Execute AWSS3Cli          ls s3://${BUCKET}/dir1/dir2/file
                        Should not contain        ${result}         testfile
                        Should not contain        ${result}         dir1
                        Should not contain        ${result}         dir2
                        Should contain            ${result}         file
