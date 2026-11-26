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
Documentation       Check whether the snapshot local data YAML files of this OM are zstd compressed
Library             OperatingSystem
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${COMPRESSED}       false
${YAML_FILES}       /data/metadata/db.snapshots/checkpointState/om.db-*.yaml

*** Keywords ***
Snapshot local data YAML files should match compression
    ${total} =         Execute    ls ${YAML_FILES} | wc -l
    Should Be True     ${total} > 0
    ${zstd_files} =    Execute    for f in ${YAML_FILES}; do head -c 4 "$f" | od -An -tx1 | tr -d ' \\n'; echo; done | grep -c 28b52ffd || true
    IF    '${COMPRESSED}' == 'true'
        Should Be Equal As Integers    ${zstd_files}    ${total}
    ELSE
        Should Be Equal As Integers    ${zstd_files}    0
    END

*** Test Cases ***
Check snapshot local data YAML compression
    Wait Until Keyword Succeeds    2min    5sec    Snapshot local data YAML files should match compression
