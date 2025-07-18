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
Documentation       Test existence of a block on a datanode
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute

*** Variables ***
${PREFIX}           ${EMPTY}
${DATANODE}         ${EMPTY}
${VOLUME}           cli-debug-volume${PREFIX}
${BUCKET}           cli-debug-bucket
${TESTFILE}         testfile
${CHECK_TYPE}       blockExistence

*** Test Cases ***
Test block existence with a block missing on a replica
    ${output} =         Execute replicas verify block existence debug tool
    ${json} =           Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${DATANODE}  Unable to find the block
