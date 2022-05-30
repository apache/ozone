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
Documentation       Test Bucket Layout backward compatibility
Library             OperatingSystem
Resource            lib.resource

*** Test Cases ***
Setup Cluster Data
    [Tags]  setup-bucket-layout-data
    Prepare Data For Xcompat Tests

Test Info Compat
    [Tags]  test-bucket-layout-compat
    ${result} =     Execute and checkrc                     ozone sh bucket info /${prefix}vol1/${prefix}default-fso    255
                    Should Contain  ${result}   NOT_SUPPORTED_OPERATION

    ${result} =     Execute and checkrc                     ozone sh bucket info /${prefix}vol1/${prefix}default-obs    255
                    Should Contain  ${result}   NOT_SUPPORTED_OPERATION

    ${result} =     Execute                     ozone sh bucket info /${prefix}vol1/${prefix}default-legacy | grep name
                    Should Contain  ${result}   ${prefix}default-legacy

#Test Listing Compat
#    [Tags]  test-bucket-layout-compat
#    ToDo:  Need to add a test case for listing buckets.

Test Bucket Create
    [Tags] test-bucket-layout-compat
    ${random} =         Generate Random String  5  [NUMBERS]
    ${result} =     Execute                     ozone sh bucket create /${prefix}vol1/${prefix}-create-${random}
    ${result} =     Execute                     ozone sh bucket info /${prefix}vol1/${prefix-}_create-${random}
                    Should Not Contain  ${result}   NOT_SUPPORTED_OPERATION