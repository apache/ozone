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
Library    ../lib/BytemanLibrary.py
Library    String


*** Variables ***
@{COMPONENTS}    datanode1    datanode2    datanode3    om1    om2    om3    recon    scm1    scm2    scm3    s3g


*** Keywords ***
Inject Fault Into All Components
    [Arguments]    ${rule_file}
    Log   Inside Setup All Byteman Agents
    FOR    ${component}    IN    @{components}
        Add Byteman Rule    ${component}    ${rule_file}
    END

Remove Fault From All Components
    [Arguments]    ${rule_file}
    Log   Inside Cleanup All Byteman Agents
    FOR    ${component}    IN    @{components}
        Remove Byteman Rule    ${component}    ${rule_file}
    END

List Byteman Rules for All Components
    Log   Inside Cleanup All Byteman Agents
    FOR    ${component}    IN    @{components}
        List Byteman Rules    ${component}
    END
