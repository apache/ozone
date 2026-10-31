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
Documentation       Snapshot Keywords
Library             BuiltIn
Library             OperatingSystem
Resource            ../lib/os.robot


*** Keywords ***
Wait For Snapshot ${command}
    [arguments]    ${bucket}    ${fromSnap}    ${toSnap}    @{keys}

    ${output} =    Wait Until Keyword Succeeds    2min    5sec
                   ...    Snapshot Diff Response Should Contain Created Keys
                          ...    ozone sh snapshot ${command} ${bucket} ${fromSnap} ${toSnap}
                          ...    ${fromSnap}    ${toSnap}    @{keys}

    [return]    ${output}


Snapshot Diff Response Should Contain Created Keys
    [arguments]    ${command}    ${fromSnap}    ${toSnap}    @{keys}

    ${output} =    Execute    ${command}

    Should Contain    ${output}    Difference between snapshot: ${fromSnap} and snapshot: ${toSnap}

    FOR  ${key}  IN  @{keys}
      Should Contain    ${output}    ${key}
    END

    [return]    ${output}
