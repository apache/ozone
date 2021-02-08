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
Documentation       Test for ozone admin cert command
Library             BuiltIn
Library             String
Resource            ../commonlib.robot
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Suite Setup         Setup Test
Test Timeout        5 minutes

*** Variables ***

*** Keywords ***
Setup Test
    Run Keyword     Kinit test user     testuser     testuser.keytab

*** Test Cases ***
List valid certificates
    ${output} =      Execute    ozone admin cert list
                     Should Contain    ${output}    valid certificates

List revoked certificates
    ${output} =      Execute    ozone admin cert list -t revoked
                     Should Contain    ${output}    Total 0 revoked certificates

Info of the cert
    ${output} =      Execute   for id in $(ozone admin cert list -c 1|grep UTC|awk '{print $1}'); do ozone admin cert info $id; done
                     Should not Contain    ${output}    Certificate not found

