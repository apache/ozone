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
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${OCKR_PREFIX}    ockr
${OMBR_PREFIX}    ombr

*** Test Cases ***
Ozone Client Key Remover
    [Setup]    Freon OCKG    ${OCKR_PREFIX}
    Freon OCKR    ${OCKR_PREFIX}

OM Bucket Remover
    [Setup]    Freon OMBG    ${OMBR_PREFIX}
    Freon OMBR    ${OMBR_PREFIX}
