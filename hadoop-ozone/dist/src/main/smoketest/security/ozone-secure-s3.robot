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
Documentation       Smoke test to start cluster with docker-compose environments.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}     http://s3g:9878
${TEMPDIR}          /tmp
${TEST_FILE}        NOTICE.txt

*** Keywords ***
Setup volume names
    ${random}            Generate Random String  2   [NUMBERS]
    Set Suite Variable   ${volume1}            fstest${random}
    Set Suite Variable   ${volume2}            fstest2${random}

*** Test Cases ***
Secure S3 test Success
    Run Keyword         Setup s3 tests
    ${output} =         Execute          aws s3api --endpoint-url ${ENDPOINT_URL} create-bucket --bucket bucket-test123
    ${output} =         Execute          aws s3api --endpoint-url ${ENDPOINT_URL} list-buckets
                        Should contain   ${output}         bucket-test123

Secure S3 put-object test
    ${testFilePath} =       Set Variable            ${TEMPDIR}/${TEST_FILE}
                            Copy File               ${TEST_FILE}            ${testFilePath}
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} put-object --bucket=bucket-test123 --key=tmp1/tmp2/NOTICE.txt --body=${testFilePath}
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} list-objects --bucket=bucket-test123
                            Should contain   ${output}         tmp1/tmp2/NOTICE.txt
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} put-object --bucket=bucket-test123 --key=tmp3//tmp4/NOTICE.txt --body=${testFilePath}
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} list-objects --bucket=bucket-test123
                            Should contain   ${output}         tmp3//tmp4/NOTICE.txt
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} put-object --bucket=bucket-test123 --key=//tmp5/tmp6/NOTICE.txt --body=${testFilePath}
    ${output} =             Execute                 aws s3api --endpoint ${ENDPOINT_URL} list-objects --bucket=bucket-test123
                            Should contain   ${output}         //tmp5/tmp6/NOTICE.txt

Secure S3 test Failure
    Run Keyword         Setup dummy credentials for S3
    ${rc}  ${result} =  Run And Return Rc And Output  aws s3api --endpoint-url ${ENDPOINT_URL} create-bucket --bucket bucket-test123
    Should Be True	${rc} > 0

