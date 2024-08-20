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
Documentation       Test HSync via freon CLI.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../ozone-lib/freon.robot
Test Timeout        10 minutes
Suite Setup         Get OM serviceId

*** Variables ***
${OMSERVICEID}
${VOLUME}           hsync-volume
${BUCKET}           hsync-bucket

*** Keywords ***
Get OM serviceId
    ${confKey} =        Execute And Ignore Error        ozone getconf confKey ozone.om.service.ids
    ${result} =         Evaluate                        "Configuration ozone.om.service.ids is missing" in """${confKey}"""
    IF      ${result} == ${True}
        Set Suite Variable  ${OMSERVICEID}         om
    ELSE
        Set Suite Variable  ${OMSERVICEID}         ${confKey}
    END

*** Test Cases ***
Generate key by HSYNC
    Freon DFSG    sync=HSYNC    path=ofs://${OMSERVICEID}/${VOLUME}/${BUCKET}

Generate key by HFLUSH
    Freon DFSG    sync=HFLUSH   path=ofs://${OMSERVICEID}/${VOLUME}/${BUCKET}
