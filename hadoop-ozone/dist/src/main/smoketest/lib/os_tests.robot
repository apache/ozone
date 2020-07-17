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
Resource    os.robot


*** Test Cases ***

Execute
    ${output} =        Execute      echo 42
    Should Be Equal    ${output}    42

Execute failing command
    Run Keyword And Expect Error    *    Execute    false

Execute And Ignore Error
    ${output} =        Execute And Ignore Error    echo 123 && false
    Should Be Equal    ${output}    123

Execute and checkrc
    ${output} =        Execute and checkrc    echo failure && exit 1    1
    Should Be Equal    ${output}    failure

Execute and checkrc RC mismatch
    Run Keyword And Expect Error    *    Execute and checkrc    echo failure && exit 3    1
