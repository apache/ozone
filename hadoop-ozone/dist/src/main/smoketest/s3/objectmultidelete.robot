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

Delete objects keeps non-empty FSO directories
    Pass Execution If   '${BUCKET_LAYOUT}' != 'FILE_SYSTEM_OPTIMIZED'    only applies to FSO buckets
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/fso/c/d2/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/fso/c/f2 --body /tmp/testfile
    ${result} =         Execute AWSS3APICli        delete-objects --bucket ${BUCKET} --delete 'Objects=[{Key=${PREFIX}/multidelete/fso/c/d2/},{Key=${PREFIX}/multidelete/fso/c/}]'
                        Should not contain         ${result}         Error
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/multidelete/fso/c/
                        Should contain             ${result}         ${PREFIX}/multidelete/fso/c/d2/f1
                        Should contain             ${result}         ${PREFIX}/multidelete/fso/c/f2

Delete objects removes FSO directories emptied by the same request
    Pass Execution If   '${BUCKET_LAYOUT}' != 'FILE_SYSTEM_OPTIMIZED'    only applies to FSO buckets
                        Execute                    date > /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/fso/dir/sub/f1 --body /tmp/testfile
    ${result} =         Execute AWSS3ApiCli        put-object --bucket ${BUCKET} --key ${PREFIX}/multidelete/fso/dir/f2 --body /tmp/testfile
    # Keys in listing order, each directory before its contents
    ${result} =         Execute AWSS3APICli        delete-objects --bucket ${BUCKET} --delete 'Objects=[{Key=${PREFIX}/multidelete/fso/dir/},{Key=${PREFIX}/multidelete/fso/dir/f2},{Key=${PREFIX}/multidelete/fso/dir/sub/},{Key=${PREFIX}/multidelete/fso/dir/sub/f1}]'
                        Should not contain         ${result}         Error
    ${result} =         Execute AWSS3ApiCli        list-objects --bucket ${BUCKET} --prefix ${PREFIX}/multidelete/fso/
                        Should not contain         ${result}         ${PREFIX}/multidelete/fso/dir/
