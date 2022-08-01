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
Documentation       Test freon echo RPC commands
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${PREFIX}    ${EMPTY}

*** Test Cases ***
Ozone Echo RPC Load Generator with request payload and response layload
    Freon RPCL     prefix=rpcl${PREFIX}

Ozone Echo RPC Load Generator with request payload and empty response layload
    Freon RPCL     prefix=rpcl${PREFIX} -erp=true

Ozone Echo RPC Load Generator with empty request payload and response layload
    Freon RPCL     prefix=rpcl${PREFIX} -erq=true

Ozone Echo RPC Load Generator with empty request payload and empty response layload
    Freon RPCL     prefix=rpcl${PREFIX} -erq=true -erp=true

