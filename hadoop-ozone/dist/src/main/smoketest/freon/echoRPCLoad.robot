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
${n}    1

*** Test Cases ***
[Read] Ozone Echo RPC Load Generator with request payload and response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-req=1 --payload-resp=1
                       Should contain   ${result}   Successful executions: ${n}

[Read] Ozone Echo RPC Load Generator with request payload and empty response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-req=1
                       Should contain   ${result}   Successful executions: ${n}

[Read] Ozone Echo RPC Load Generator with empty request payload and response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-resp=1
                       Should contain   ${result}   Successful executions: ${n}

[Read] Ozone Echo RPC Load Generator with empty request payload and empty response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n}
                       Should contain   ${result}   Successful executions: ${n}

[Write] Ozone Echo RPC Load Generator with request payload and response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-req=1 --payload-resp=1 --ratis
                       Should contain   ${result}   Successful executions: ${n}

[Write] Ozone Echo RPC Load Generator with request payload and empty response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-req=1 --ratis
                       Should contain   ${result}   Successful executions: ${n}

[Write] Ozone Echo RPC Load Generator with empty request payload and response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --payload-resp=1 --ratis
                       Should contain   ${result}   Successful executions: ${n}

[Write] Ozone Echo RPC Load Generator with empty request payload and empty response payload
    ${result} =        Execute          ozone freon ome -t=1 -n=${n} --ratis
                       Should contain   ${result}   Successful executions: ${n}
