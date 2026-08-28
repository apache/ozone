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
Documentation       Test ozone Debug CLI for EC(3,2) replicated keys
Library             OperatingSystem
Library             Process
Resource            ../lib/os.robot
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute
Suite Setup         Create Volume Bucket

*** Variables ***
${PREFIX}           ${EMPTY}
${VOLUME}           cli-debug-ec-volume${PREFIX}
${BUCKET}           cli-debug-ec-bucket
${TESTFILE}         testfile
${EC_DATA}          3
${EC_PARITY}        2
${OM_SERVICE_ID}    %{OM_SERVICE_ID}
# single-block stripe: one full data block (1048576), other data blocks=0, parity blocks mirrors data1
${EC32_SINGLE_BLOCK_STRIPE_SIZES}        [[1048576,0,0,1048576,1048576]]
# 3 MiB full stripe (3*1048576): all 5 replicas are one full 1024k chunk
${EC32_FULL_STRIPE_SIZES}           [[1048576,1048576,1048576,1048576,1048576]]
# group0: full stripe; group1: partial-block stripe with one 1000000 B data block
${EC32_FULL_AND_PARTIAL_BLOCK_STRIPE_SIZES}    [[1048576,1048576,1048576,1048576,1048576],[1000000,0,0,1000000,1000000]]
# multi-block stripe: data1-2=1048576, data3=2500000%1048576=402848, parity mirrors data1
${EC32_MULTI_BLOCK_STRIPE_SIZES}    [[1048576,1048576,402848,1048576,1048576]]

*** Test Cases ***
Test ozone debug replicas chunk-info single-block stripe
    # 1*1048576: one full data block in a single-block stripe
    Create EC key     ${EC_DATA}    ${EC_PARITY}    1048576
    Verify chunk-info block sizes    ${EC32_SINGLE_BLOCK_STRIPE_SIZES}

Test ozone debug replicas chunk-info full stripe
    # 3*1048576: EC_DATA full 1024k chunks = one complete stripe
    Create EC key     ${EC_DATA}    ${EC_PARITY}    3145728
    Verify chunk-info block sizes    ${EC32_FULL_STRIPE_SIZES}

Test ozone debug replicas chunk-info full stripe and partial-block stripe
    # 3*1048576 + 1000000: one full stripe plus a partial-block stripe (1000000 B data block)
    Create EC key     ${EC_DATA}    ${EC_PARITY}    4145728
    Verify chunk-info block sizes    ${EC32_FULL_AND_PARTIAL_BLOCK_STRIPE_SIZES}

Test ozone debug replicas chunk-info multi-block stripe
    # 2*1048576 + 402848: two full data blocks plus a 402848 B partial data block
    Create EC key     ${EC_DATA}    ${EC_PARITY}    2500000
    Verify chunk-info block sizes    ${EC32_MULTI_BLOCK_STRIPE_SIZES}
