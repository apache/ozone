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
    ${dir} =    Set Variable    /${VOLUME}/ecbucket/dir
    ${key} =    Set Variable    ${dir}/${size}
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}
    Key Should Match Local File    ${key}      ${file}
    Verify Key EC Replication Config    ${key}    RS    3    2    1048576
    Verify Key EC Replication Config    ${dir}    RS    3    2    1048576

Get Disk Usage of File with EC RS Replication
                                     [arguments]    ${fileLength}    ${dataChunkCount}    ${parityChunkCount}    ${ecChunkSize}
    ${ecChunkSize} =                 Evaluate   ${ecChunkSize} * 1024
    # the formula comes from https://github.com/apache/ozone/blob/master/hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/helpers/QuotaUtil.java#L42-L60
    ${dataStripeSize} =              Evaluate   ${dataChunkCount} * ${ecChunkSize} * 1024
    ${fullStripes} =                 Evaluate   ${fileLength}/${dataStripeSize}
    ${fullStripes} =                 Convert To Integer   ${fullStripes}
    # rounds to ones digit
    ${fullStripes} =                 Convert to Number    ${fullStripes}    0
    ${partialFirstChunk} =           Evaluate   ${fileLength} % ${dataStripeSize}
    ${ecChunkSize} =                 Convert To Integer   ${ecChunkSize}
    ${partialFirstChunk} =           Convert To Integer   ${partialFirstChunk}
    ${partialFirstChunkOptions} =    Create List   ${ecChunkSize}   ${partialFirstChunk}
    ${partialFirstChunk} =           Evaluate   min(${partialFirstChunkOptions})
    ${replicationOverhead} =         Evaluate   ${fullStripes} * 2 * 1024 * 1024 + ${partialFirstChunk} * 2
    ${expectedDiskUsage} =           Evaluate   ${fileLength} + ${replicationOverhead}
    # Convert float to int
    ${expectedDiskUsage} =           Convert To Integer    ${expectedDiskUsage}
    ${expectedDiskUsage} =           Convert To String    ${expectedDiskUsage}
                                     [return]             ${expectedDiskUsage}


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

Create Key in Default Bucket
    ${size} =    Set Variable    1mb
    ${dir} =     Set Variable    /${VOLUME}/default/dir
    ${key} =     Set Variable    ${dir}/${size}
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}
    Key Should Match Local File    ${key}      ${file}
    Verify Key Replica Replication Config   ${key}     RATIS    THREE
    Verify Key Replica Replication Config   ${dir}     RATIS    THREE

Create Key in Ratis Bucket
    ${size} =    Set Variable    1mb
    ${dir} =     Set Variable    /${VOLUME}/ratis/dir
    ${key} =     Set Variable    ${dir}/${size}
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}
    Key Should Match Local File    ${key}      ${file}
    Verify Key Replica Replication Config   ${key}     RATIS    THREE
    Verify Key Replica Replication Config   ${dir}     RATIS    THREE

Create Ratis Key In EC Bucket
    ${size} =    Set Variable    1mb
    ${dir} =     Set Variable    /${VOLUME}/ecbucket/dir2
    ${key} =     Set Variable    ${dir}/${size}Ratis
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}    --replication=THREE --type=RATIS
    Key Should Match Local File   ${key}    ${file}
    Verify Key Replica Replication Config   ${key}    RATIS   THREE
    Verify Key EC Replication Config    ${dir}    RS    3    2    1048576

Create EC Key In Ratis Bucket
    ${size} =    Set Variable    1mb
    ${dir} =     Set Variable    /${VOLUME}/ratis/dir2
    ${key} =     Set Variable    ${dir}/${size}EC
    ${file} =    Set Variable    /tmp/${size}
    Create Key    ${key}    ${file}    --replication=rs-3-2-1024k --type=EC 
    Key Should Match Local File    ${key}    ${file}
    Verify Key EC Replication Config    ${key}    RS    3    2    1048576
    Verify Key Replica Replication Config   ${dir}     RATIS    THREE

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

Check disk usage after create a file which uses EC replication type
                   ${vol} =    Generate Random String   8  [LOWER]
                ${bucket} =    Generate Random String   8  [LOWER]
                               Execute                  ozone sh volume create /${vol}
                               Execute                  ozone sh bucket create /${vol}/${bucket} --type EC --replication rs-3-2-1024k
                               Execute                  ozone fs -put NOTICE.txt /${vol}/${bucket}/PUTFILE2.txt
    ${expectedFileLength} =    Execute                  stat -c %s NOTICE.txt
     ${expectedDiskUsage} =    Get Disk Usage of File with EC RS Replication    ${expectedFileLength}    3    2    1024
                ${result} =    Execute                  ozone fs -du /${vol}/${bucket}
                               Should contain           ${result}         PUTFILE2.txt
                               Should contain           ${result}         ${expectedFileLength}
                               Should contain           ${result}         ${expectedDiskUsage}
