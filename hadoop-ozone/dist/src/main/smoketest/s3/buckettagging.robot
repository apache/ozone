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
Documentation       S3 gateway bucket tagging tests with aws cli
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup bucket tagging tests

*** Variables ***
${ENDPOINT_URL}     http://s3g:9878
${OZONE_TEST}       true
${BUCKET}           generated
${LINK_BUCKET}      link-bucket-tagging

*** Keywords ***
Setup bucket tagging tests
    Setup s3 tests
    Setup link bucket for tagging    ${LINK_BUCKET}

*** Test Cases ***

Get bucket tagging without tags
    ${result} =     Execute AWSS3APICli and checkrc    get-bucket-tagging --bucket ${BUCKET}    255
                    Should contain                     ${result}                      NoSuchTagSet

Put bucket tagging
                    Execute AWSS3ApiCli                put-bucket-tagging --bucket ${BUCKET} --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'

Get bucket tagging
    ${result} =     Execute AWSS3ApiCli                get-bucket-tagging --bucket ${BUCKET}
                    Should contain                     ${result}                       TagSet
    ${tagCount} =   Execute and checkrc                echo '${result}' | jq '.TagSet | length'   0
                    Should Be Equal                    ${tagCount}                     1

Put bucket tagging overwrites existing tags
                    Execute AWSS3ApiCli                put-bucket-tagging --bucket ${BUCKET} --tagging '{"TagSet": [{ "Key": "tag-key2", "Value": "tag-value2" },{ "Key": "tag-key3", "Value": "tag-value3" }]}'

Get bucket tagging after overwrite
    ${result} =     Execute AWSS3ApiCli                get-bucket-tagging --bucket ${BUCKET}
                    Should contain                     ${result}                        TagSet
    ${tagCount} =   Execute and checkrc                echo '${result}' | jq '.TagSet | length'    0
                    Should Be Equal                    ${tagCount}                      2

Put bucket tagging on nonexistent bucket
    ${result} =     Execute AWSS3APICli and checkrc    put-bucket-tagging --bucket ${PREFIX}-missing-bucket-tagging --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'    255
                    Should contain                     ${result}                        NoSuchBucket

Delete bucket tagging
                    Execute AWSS3ApiCli    delete-bucket-tagging --bucket ${BUCKET}

Get bucket tagging after delete returns NoSuchTagSet
    ${result} =     Execute AWSS3APICli and checkrc    get-bucket-tagging --bucket ${BUCKET}    255
                    Should contain                     ${result}                         NoSuchTagSet

Get bucket tagging on link bucket without tags
    ${result} =     Execute AWSS3APICli and checkrc    get-bucket-tagging --bucket ${LINK_BUCKET}    255
                    Should contain                     ${result}                      NoSuchTagSet

Put bucket tagging on link bucket
                    Execute AWSS3ApiCli                put-bucket-tagging --bucket ${LINK_BUCKET} --tagging '{"TagSet": [{ "Key": "tag-key1", "Value": "tag-value1" }]}'

Get bucket tagging on link bucket
    ${result} =     Execute AWSS3ApiCli                get-bucket-tagging --bucket ${LINK_BUCKET}
                    Should contain                     ${result}                       TagSet
    ${tagCount} =   Execute and checkrc                echo '${result}' | jq '.TagSet | length'   0
                    Should Be Equal                    ${tagCount}                     1

Delete bucket tagging on link bucket
                    Execute AWSS3ApiCli                delete-bucket-tagging --bucket ${LINK_BUCKET}

Get bucket tagging on link bucket after delete
    ${result} =     Execute AWSS3APICli and checkrc    get-bucket-tagging --bucket ${LINK_BUCKET}    255
                    Should contain                     ${result}                         NoSuchTagSet
