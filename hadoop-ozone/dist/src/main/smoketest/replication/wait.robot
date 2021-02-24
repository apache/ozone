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
Documentation       Wait for replication to succeed
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${container}    1
${count}        3

*** Keywords ***
Check Container Replicated
    ${output} =    Execute    ozone admin container info --json "${container}" | jq '.pipeline.nodes | length'
    Should Be Equal    ${output}   ${count}

*** Test Cases ***
Wait Until Container Replicated
    Wait Until Keyword Succeeds    5min    10sec    Check Container Replicated
