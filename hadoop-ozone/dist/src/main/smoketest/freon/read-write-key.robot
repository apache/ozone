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
Documentation       Test freon read/write key commands
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${PREFIX}    ${EMPTY}


*** Test Cases ***
Pre-generate 100 keys of size 1 byte each to Ozone
    ${result} =        Execute          ozone freon ork -n 1 -t 10 -r 100 --size 1 -v voltest -b buckettest -p performanceTest

Read 10 keys from pre-generated keys
    ${keysCount} =     BuiltIn.Set Variable   10
    ${result} =        Execute          ozone freon ockrw -n ${keysCount} -t 10 -r 100 -v voltest -b buckettest -p performanceTest
                       Should contain   ${result}   Successful executions: ${keysCount}

Read 10 keys' metadata from pre-generated keys
    ${keysCount} =     BuiltIn.Set Variable   10
    ${result} =        Execute          ozone freon ockrw -n ${keysCount} -t 10 -m -r 100 -v voltest -b buckettest -p performanceTest
                       Should contain   ${result}   Successful executions: ${keysCount}

Write 10 keys of size 1 byte each from key index 0 to 99
    ${keysCount} =     BuiltIn.Set Variable   10
    ${size} =          BuiltIn.Set Variable   1
    ${result} =        Execute          ozone freon ockrw -n ${keysCount} -t 10 --percentage-read 0 --size ${size} -r 100 -v voltest -b buckettest -p performanceTest
                       Should contain   ${result}   Successful executions: ${keysCount}
    ${keyName} =       Execute          echo -n '1' | md5sum | head -c 7
    ${result} =        Execute          ozone sh key info /voltest/buckettest/performanceTest/${keyName}
                       Should contain   ${result}   \"dataSize\" : 1


Run 90 % of read-key tasks and 10 % of write-key tasks for 10 keys from pre-generated keys
    ${keysCount} =     BuiltIn.Set Variable   10
    ${result} =        Execute          ozone freon ockrw -n ${keysCount} -t 10 --percentage-read 90 -r 100 -v voltest -b buckettest -p performanceTest
                       Should contain   ${result}   Successful executions: ${keysCount}

