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

*** Test Cases ***
Testing OM DB Size Reduction After Compaction
    # Test keys are already created and flushed
    # Delete keys to create tombstones that need compaction
    Delete Test Keys
    
    ${size_before_compaction} =    Get OM DB SST Files Size

    Compact OM DB Column Family    fileTable
    Compact OM DB Column Family    deletedTable
    Compact OM DB Column Family    deletedDirectoryTable
    
    ${size_after_compaction} =    Get OM DB SST Files Size

    Should Be True    ${size_after_compaction} < ${size_before_compaction}
    ...    OM DB size should be reduced after compaction. Before: ${size_before_compaction}, After: ${size_after_compaction}
