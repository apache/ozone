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
Documentation       Test HSync during upgrade
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Default Tags        pre-finalized-hsync-tests
Suite Setup         Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Variables ***
${OMSERVICEID}
${VOLUME}
${BUCKET}
${KEY}

*** Keywords ***
Get OM serviceId
    ${confKey} =        Execute And Ignore Error        ozone getconf confKey ozone.om.service.ids
    ${result} =         Evaluate                        "Configuration ozone.om.service.ids is missing" in """${confKey}"""
    IF      ${result} == ${True}
        Set Suite Variable  ${OMSERVICEID}         om
    ELSE
        Set Suite Variable  ${OMSERVICEID}         ${confKey}
    END
Create volume for upgrade test
    ${random} =     Generate Random String  5  [LOWER]
    ${volume} =     Set Variable        vol-${random}
    ${result} =     Execute             ozone sh volume create /${volume}
                    Should not contain  ${result}       Failed
    Set Suite Variable      ${VOLUME}           ${volume}

Create bucket for upgrade test
    ${random} =     Generate Random String  5  [LOWER]
    ${bucket} =     Set Variable        buc-${random}
    ${result} =     Execute             ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${volume}/${bucket}
                    Should not contain  ${result}       Failed
    Set Suite Variable      ${BUCKET}           ${bucket}

Create key for upgrade test
    ${random} =     Generate Random String  5  [LOWER]
    ${key} =        Set Variable        key-${random}
    ${result} =     Execute             ozone sh key put /${volume}/${bucket}/${key} /etc/hosts
    Set Suite Variable      ${KEY}          ${key}

*** Test Cases ***
Test HSync Prior To Finalization
    Get OM serviceId
    Create volume for upgrade test
    Create bucket for upgrade test
    Create key for upgrade test
    ${result} =     Execute and checkrc        ozone debug recover --path=ofs://${OMSERVICEID}/${VOLUME}/${BUCKET}/${KEY}    255
                    Should contain  ${result}  It belongs to the layout feature HBASE_SUPPORT, whose layout version is 7
    ${result} =     Execute and checkrc        ozone debug recover --path=o3fs://${BUCKET}.${VOLUME}.${OMSERVICEID}/${KEY}    255
                    Should contain  ${result}  It belongs to the layout feature HBASE_SUPPORT, whose layout version is 7
