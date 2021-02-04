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
Documentation       Test ozone Debug CLI
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        2 minute
Suite Setup         Write key
*** Variables ***

*** Keywords ***
Write key
    Execute             ozone sh volume create o3://om/vol1 --space-quota 100TB --namespace-quota 100
    Execute             ozone sh bucket create o3://om/vol1/bucket1
    Execute             ozone sh key put o3://om/vol1/bucket1/debugKey /opt/hadoop/NOTICE.txt

*** Test Cases ***
Test ozone debug
    ${result} =     Execute             ozone debug chunkinfo o3://om/vol1/bucket1/debugKey | jq -r '.KeyLocations[0][0].Locations'
                    Should contain      ${result}       files
    ${result} =     Execute             ozone debug chunkinfo o3://om/vol1/bucket1/debugKey | jq -r '.KeyLocations[0][0].Locations.files[0]'
                    File Should Exist   ${result}

