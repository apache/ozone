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
Documentation       Test Bucket Layout during upgrade
Library             OperatingSystem
Resource            lib.resource

*** Test Cases ***
Test Bucket Layout Prior To Finalization
    [Tags]  pre-finalized-bucket-layout-tests
    Execute         ozone sh volume create /bucket-layout-test
    ${result} =     Execute and checkrc                     ozone sh bucket create bucket-layout-test/fso-bucket --layout FILE_SYSTEM_OPTIMIZED 255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute and checkrc                     ozone sh bucket create bucket-layout-test/obs-bucket --layout OBJECT_STORE          255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute                     ozone sh bucket create bucket-layout-test/legacy-bucket --layout LEGACY
                    Should Not contain  ${result}   NOT_SUPPORTED_OPERATION



Test Bucket Layout After Finalization
    [Tags]  post-finalized-bucket-layout-tests
    Execute         ozone sh volume create /bucket-layout-test-new
    ${result} =     Execute                         ozone sh bucket create bucket-layout-test-new/fso-bucket-new --layout FILE_SYSTEM_OPTIMIZED
                    Should Not contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute                         ozone sh bucket info bucket-layout-test-new/fso-bucket-new | grep bucketLayout
                    Should contain      ${result}   FILE_SYSTEM_OPTIMIZED

    ${result} =     Execute                         ozone sh bucket create bucket-layout-test-new/obs-bucket-new --layout OBJECT_STORE
                    Should Not contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute                         ozone sh bucket info bucket-layout-test-new/obs-bucket-new | grep bucketLayout
                    Should contain      ${result}   OBJECT_STORE

    ${result} =     Execute                         ozone sh bucket create bucket-layout-test-new/legacy-bucket-new --layout LEGACY
                    Should Not contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute                         ozone sh bucket info bucket-layout-test-new/legacy-bucket-new | grep bucketLayout
                    Should contain      ${result}   LEGACY
