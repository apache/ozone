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
    ${result} =        Execute          ozone freon ockrw -n 1 -t 10 -r -i 0 -j 99 -g 1 -v voltest -b buckettest -p performanceTest

Read 10 keys from pre-generated keys
    ${result} =        Execute          ozone freon ockrw -n 10 -t 10 -s 0 -e 9 -v voltest -b buckettest -p performanceTest

Read 10 keys' metadata from pre-generated keys
    ${result} =        Execute          ozone freon ockrw -n 10 -t 10 -m -s 0 -e 9 -v voltest -b buckettest -p performanceTest

Write 10 keys of size 1 byte each to replace parts of the pre-generated keys
    ${result} =        Execute          ozone freon ockrw -n 10 -t 10 -i 0 -j 9 -g 1 -v voltest -b buckettest -p performanceTest

Run 90 % of read-key tasks and 10 % of write-key tasks from pre-generated keys
    ${result} =        Execute          ozone freon ockrw -n 10 -t 10 -x --percentage-read 90 -s 0 -e 9 -i 10 -j 19 -g 1 -v voltest -b buckettest -p performanceTest

