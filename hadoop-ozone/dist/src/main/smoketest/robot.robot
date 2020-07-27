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
Documentation       Smoketest for Robot functions
Resource            commonlib.robot
Test Timeout        5 minutes

*** Test Cases ***

Ensure Leading without Leading
    ${result} =        Ensure Leading    /    a/b
    Should Be Equal    ${result}         /a/b

Ensure Leading with Leading
    ${result} =        Ensure Leading    _   _a_b_c
    Should Be Equal    ${result}         _a_b_c

Ensure Leading for empty
    ${result} =        Ensure Leading    |    ${EMPTY}
    Should Be Equal    ${result}         |


Ensure Trailing without Trailing
    ${result} =    Ensure Trailing    .    x.y.z
    Should Be Equal    ${result}      x.y.z.

Ensure Trailing with Trailing
    ${result} =    Ensure Trailing    x   axbxcx
    Should Be Equal    ${result}      axbxcx

Ensure Trailing for empty
    ${result} =        Ensure Trailing   =    ${EMPTY}
    Should Be Equal    ${result}         =


Format o3fs URL without path
    ${result} =    Format o3fs URL    vol1    bucket1
    Should Be Equal    ${result}    o3fs://bucket1.vol1/

Format o3fs URL with path
    ${result} =    Format o3fs URL    vol1    bucket1    dir/file
    Should Be Equal    ${result}    o3fs://bucket1.vol1/dir/file


Format ofs URL without path
    ${result} =    Format ofs URL    vol1    bucket1
    Should Be Equal    ${result}    ofs://vol1/bucket1

Format ofs URL with path
    ${result} =    Format ofs URL    vol1    bucket1    dir/file
    Should Be Equal    ${result}    ofs://vol1/bucket1/dir/file


Format FS URL with ofs scheme
    ${result} =    Format FS URL    ofs    vol1    bucket1
    ${expected} =  Format ofs URL    vol1    bucket1
    Should Be Equal    ${result}    ${expected}

Format FS URL with o3fs scheme
    ${result} =    Format FS URL    o3fs    vol1    bucket1
    ${expected} =  Format o3fs URL    vol1    bucket1
    Should Be Equal    ${result}    ${expected}

Format FS URL with unsupported scheme
    ${result} =    Run Keyword And Expect Error   *    Format FS URL    http    org   apache
    Should Contain    ${result}    http
    Should Contain    ${result}    nsupported

