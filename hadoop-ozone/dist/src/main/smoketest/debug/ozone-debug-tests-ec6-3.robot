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
Documentation       Test ozone Debug CLI for EC(6,3) replicated keys
Library             OperatingSystem
Library             Process
Resource            ../ec/lib.resource
Resource            ../lib/os.robot
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute
Suite Setup         Run Keywords    Wait Until Keyword Succeeds    2min    10sec    Has Enough Datanodes    9
...                     AND    Create Volume Bucket

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-ec6-volume${PREFIX}
${BUCKET}           cli-debug-ec6-bucket
${TESTFILE}         testfile
${EC_DATA}          6
${EC_PARITY}        3
${OM_SERVICE_ID}    %{OM_SERVICE_ID}
# single-block stripe: one full data block (1048576), other data blocks=0, parity mirrors data1
${EC63_SINGLE_BLOCK_STRIPE_SIZES}        [[1048576,0,0,0,0,0,1048576,1048576,1048576]]
# 6 MiB full stripe (6*1048576): all 9 replicas are one full 1024k chunk
${EC63_FULL_STRIPE_SIZES}           [[1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576]]
# group0: full stripe; group1: partial-block stripe with one 1000000 B data block
${EC63_FULL_AND_PARTIAL_BLOCK_STRIPE_SIZES}    [[1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576],[1000000,0,0,0,0,0,1000000,1000000,1000000]]
# multi-block stripe: data1-3=1048576, data4=3500000%1048576=354272, data5-6=0, parity mirrors data1
${EC63_MULTI_BLOCK_STRIPE_SIZES}    [[1048576,1048576,1048576,354272,0,0,1048576,1048576,1048576]]

*** Test Cases ***
Test ozone debug replicas chunk-info single-block stripe
    # 1*1048576: one full data block in a single-block stripe
    Create EC key     ${EC_DATA}    ${EC_PARITY}    1048576
    Verify chunk-info block sizes    ${EC63_SINGLE_BLOCK_STRIPE_SIZES}

Test ozone debug replicas chunk-info full stripe
    # 6*1048576: EC_DATA full 1024k chunks = one complete stripe
    Create EC key     ${EC_DATA}    ${EC_PARITY}    6291456
    Verify chunk-info block sizes    ${EC63_FULL_STRIPE_SIZES}

Test ozone debug replicas chunk-info full stripe and partial-block stripe
    # 6*1048576 + 1000000: one full stripe plus a partial-block stripe (1000000 B data block)
    Create EC key     ${EC_DATA}    ${EC_PARITY}    7291456
    Verify chunk-info block sizes    ${EC63_FULL_AND_PARTIAL_BLOCK_STRIPE_SIZES}

Test ozone debug replicas chunk-info multi-block stripe
    # 3*1048576 + 354272: three full data blocks plus a 354272 B partial data block
    Create EC key     ${EC_DATA}    ${EC_PARITY}    3500000
    Verify chunk-info block sizes    ${EC63_MULTI_BLOCK_STRIPE_SIZES}
