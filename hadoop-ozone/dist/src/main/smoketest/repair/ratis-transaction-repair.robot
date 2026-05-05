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
Documentation       Test recovering from OM crash due to transaction failure
Library             OperatingSystem
Library             BuiltIn
Library             Process
Resource            ../lib/os.robot
Resource            ../ozone-fi/BytemanKeywords.robot

*** Variables ***
${VOLUME}               test-txn-vol
${BAD_BUCKET}           bucket-crash-1
${CRASH_RULE}           /opt/hadoop/share/ozone/byteman/fail-create-bucket.btm
${TIMEOUT}              10 seconds

*** Test Cases ***
Verify OM crash at bucket create
    Inject Fault Into OMs Only      ${CRASH_RULE}
    Execute         ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Run Process     ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BAD_BUCKET}    timeout=${TIMEOUT}    shell=True
    Remove Fault From OMs Only      ${CRASH_RULE}
