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
Resource            string.robot


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

