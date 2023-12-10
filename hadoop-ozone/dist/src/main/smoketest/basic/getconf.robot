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
Documentation       Test 'ozone getconf' command
Resource            ../lib/os.robot
Test Timeout        5 minutes

*** Test Cases ***
Get OM
    ${result} =      Execute                ozone getconf ozonemanagers
                     Should contain   ${result}   om
    ${result} =      Execute                ozone getconf -ozonemanagers
                     Should contain   ${result}   om

Get SCM
    ${result} =      Execute                ozone getconf storagecontainermanagers
                     Should contain   ${result}   scm
    ${result} =      Execute                ozone getconf -storagecontainermanagers
                     Should contain   ${result}   scm

Get existing config key
    ${result} =      Execute                ozone getconf confKey ozone.om.address
                     Should contain    ${result}   om
                     Should not contain   ${result}   is missing
    ${result} =      Execute                ozone getconf -confKey ozone.om.address
                     Should contain    ${result}   om
                     Should not contain   ${result}   is missing

Get undefined config key
    ${result} =      Execute and checkrc    ozone getconf confKey no-such-config-key    255
                     Should contain   ${result}   Configuration no-such-config-key is missing
    ${result} =      Execute and checkrc    ozone getconf -confKey no-such-config-key    255
                     Should contain   ${result}   Configuration no-such-config-key is missing
