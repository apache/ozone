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
Documentation       Test checksums in case of a stale datanode
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ozone-debug-keywords.robot
Test Timeout        5 minute

*** Variables ***
${PREFIX}              ${EMPTY}
${STALE_DATANODE}      ${EMPTY}
${VOLUME}              cli-debug-volume${PREFIX}
${BUCKET}              cli-debug-bucket
${TESTFILE}            testfile
${CHECK_TYPE}          checksum

*** Test Cases ***
Test checksums with a stale datanode
    ${output} =         Execute replicas verify checksums debug tool
    ${json} =           Parse replicas verify JSON output    ${output}
    Check to Verify Replicas    ${json}  ${CHECK_TYPE}  ${STALE_DATANODE}  UNAVAILABLE
