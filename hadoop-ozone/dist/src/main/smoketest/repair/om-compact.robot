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
Documentation       Test for OM DB Compaction Repair Tool
Library             OperatingSystem
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        10 minutes

*** Variables ***
${OM_DB_PATH}       /data/metadata/om.db

*** Keywords ***
Delete Test Keys
    Execute    ozone fs -rm -R -skipTrash ofs://${OM_SERVICE_ID}/vol1/bucket1

Get OM DB SST Files Size
    ${output} =    Execute    find ${OM_DB_PATH} -name '*.sst' -exec du -b {} + | awk '{sum += $1} END {print sum}'
    ${sst_size} =  Convert To Integer    ${output}
    [Return]      ${sst_size}

Compact OM DB Column Family
    [Arguments]        ${column_family}
    Execute    ozone repair om compact --cf=${column_family} --service-id ${OM_SERVICE_ID} --node-id om1

SST Size Should Be Reduced
    [Arguments]    ${original_size}
    ${current_size} =    Get OM DB SST Files Size
    Should Be True    ${current_size} < ${original_size}
    ...    OM DB size should be reduced after compaction. Before: ${original_size}, After: ${current_size}

*** Test Cases ***
Testing OM DB Size Reduction After Compaction
    # Test keys are already created, deleted and flushed by the test script
    ${size_before_compaction} =    Get OM DB SST Files Size

    Compact OM DB Column Family    fileTable
    Compact OM DB Column Family    deletedTable
    Compact OM DB Column Family    deletedDirectoryTable

    # Compaction is asynchronous, poll until size is reduced
    Wait Until Keyword Succeeds    60sec    5sec    SST Size Should Be Reduced    ${size_before_compaction}
