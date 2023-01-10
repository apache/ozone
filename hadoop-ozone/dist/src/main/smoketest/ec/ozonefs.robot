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
Documentation       Test Ozone FS EC type of file
Library             OperatingSystem
Library             String
Resource            lib.resource
Test Timeout        5 minutes

*** Test Cases ***
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


*** Keywords ***

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


