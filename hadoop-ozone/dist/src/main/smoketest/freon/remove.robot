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
Documentation       Test freon data remove commands
Resource            ../lib/os.robot
Test Timeout        5 minutes

*** Variables ***
${OCKR_PREFIX}    ockr
${OMBR_PREFIX}    ombr

*** Test Cases ***
Ozone Client Key Remover
    [Setup]            Ozone Client Key Generator For Remover    ${OCKR_PREFIX}
    ${result} =        Execute                ozone freon ockr ${OM_HA_PARAM} -t=1 -n=1 -p ${OCKR_PREFIX}
                       Should contain         ${result}   Successful executions: 1

OM Bucket Remover
    [Setup]            OM Bucket Generator For Remover           ${OMBR_PREFIX}
    ${result} =        Execute                ozone freon ombr ${OM_HA_PARAM} -t=1 -n=1 -p ${OMBR_PREFIX}
                       Should contain         ${result}   Successful executions: 1

*** Keywords ***
Ozone Client Key Generator For Remover
    [Arguments]        ${PREFIX}
    Execute            ozone freon ockg ${OM_HA_PARAM} -t=1 -n=1 -p ${PREFIX}

OM Bucket Generator For Remover
    [Arguments]        ${PREFIX}
    Execute            ozone freon ombg ${OM_HA_PARAM} -t=1 -n=1 -p ${PREFIX}