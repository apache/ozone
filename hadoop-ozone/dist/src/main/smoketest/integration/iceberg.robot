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
Documentation       Test for Iceberg integration
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${BUCKET}    warehouse

*** Test Cases ***
Verify Warehouse contents
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
    ${output} =    Execute    ozone sh key list /s3v/${BUCKET} | jq -r '.[].name' | sort
    Should Contain    ${output}    nyc/taxis/data/
    Should Contain    ${output}    nyc/taxis/metadata/
