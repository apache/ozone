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
Documentation       Test EC shell commands
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Resource            lib.resource
Suite Setup         Prepare For Tests

*** Variables ***
${PREFIX}    ${EMPTY}
${VOLUME}    vol${PREFIX}

*** Keywords ***
Create Key In EC Bucket
    [arguments]    ${size}
    ${key} =    Set Variable    /${VOLUME}/ecbucket/${size}
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}
    Key Should Match Local File    ${key}      ${file}
    Verify Key EC Replication Config    ${key}    RS    3    2    1048576

*** Test Cases ***
Test Bucket Creation
    ${result} =     Execute             ozone sh volume create /${VOLUME}
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${VOLUME}/default
                    Should not contain  ${result}       Failed
                    Verify Bucket Empty Replication Config      /${VOLUME}/default
    ${result} =     Execute             ozone sh bucket create --replication 3 --type RATIS /${VOLUME}/ratis
                    Should not contain  ${result}       Failed
                    Verify Bucket Replica Replication Config    /${VOLUME}/ratis   RATIS   THREE
    ${result} =     Execute             ozone sh bucket create --replication rs-3-2-1024k --type EC /${VOLUME}/ecbucket
                    Should not contain  ${result}       Failed
                    Verify Bucket EC Replication Config    /${VOLUME}/ecbucket    RS    3    2    1048576

Create 1MB Key In EC Bucket
    Create Key In EC Bucket    1mb

Create 2MB Key In EC Bucket
    Create Key In EC Bucket    2mb

Create 3MB Key In EC Bucket
    Create Key In EC Bucket    3mb

Create 100MB Key In EC Bucket
    Create Key In EC Bucket    100mb

Create Key in Ratis Bucket
    ${size} =    Set Variable    1mb
    ${key} =    Set Variable    /${VOLUME}/default/${size}
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}
    Key Should Match Local File    ${key}      ${file}
    Verify Key Replica Replication Config   ${key}     RATIS    THREE

Create Ratis Key In EC Bucket
    ${size} =    Set Variable    1mb
    ${key} =    Set Variable    /${VOLUME}/ecbucket/${size}Ratis
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}    --replication=THREE --type=RATIS
    Key Should Match Local File   ${key}    ${file}
    Verify Key Replica Replication Config   ${key}    RATIS   THREE

Create EC Key In Ratis Bucket
    ${size} =    Set Variable    1mb
    ${key} =    Set Variable    /${VOLUME}/ratis/${size}EC
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}    --replication=rs-3-2-1024k --type=EC 
    Key Should Match Local File    ${key}    ${file}
    Verify Key EC Replication Config    ${key}    RS    3    2    1048576

Test Invalid Replication Parameters
    ${message} =    Execute And Ignore Error    ozone sh bucket create --replication=rs-3-2-1024k --type=RATIS /${VOLUME}/foo
                    Should contain              ${message}          RATIS
                    Should contain              ${message}          rs-3-2-1024k
                    Should contain              ${message}          not supported
    ${message} =    Execute And Ignore Error    ozone sh key put --replication=rs-6-3-1024k --type=RATIS /${VOLUME}/foo/bar /tmp/1mb
                    Should contain              ${message}          RATIS
                    Should contain              ${message}          rs-6-3-1024k
                    Should contain              ${message}          not supported
    ${message} =    Execute And Ignore Error    ozone sh bucket create --replication=rs-6-3-1024k --type=STAND_ALONE /${VOLUME}/foo
                    Should contain              ${message}          STAND_ALONE
                    Should contain              ${message}          Invalid value
    ${message} =    Execute And Ignore Error    ozone sh key put --replication=rs-3-2-1024k --type=STAND_ALONE /${VOLUME}/foo/bar /tmp/1mb
                    Should contain              ${message}          STAND_ALONE
                    Should contain              ${message}          Invalid value

Invalid Replication With Misconfigured Client
    # client disabled replication config validation
    # 2-1 is unsupported EC scheme
    ${message} =    Execute And Ignore Error    ozone sh -Dozone.replication.allowed-configs="" bucket create --replication=rs-2-1-1024k --type=EC /${VOLUME}/invalid
                    Should contain              ${message}          INVALID_REQUEST Invalid replication config
    # 1024 is unsupported EC chunk size
    ${message} =    Execute And Ignore Error    ozone sh -Dozone.replication.allowed-configs="" key put --replication=rs-3-2-1024 --type=EC /${VOLUME}/ecbucket/invalid /tmp/1mb
                    Should contain              ${message}          INVALID_REQUEST Invalid replication config
