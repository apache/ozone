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
Documentation       Test ozone admin datanode command
Library             BuiltIn
Resource            ../commonlib.robot
Suite Setup         Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Keywords ***
Assert Output
    [arguments]    ${output}    ${expected}    ${uuid}
    Should contain	    ${output}    Datanode: ${uuid}
    ${datanodes} =	    Get Lines Containing String    ${output}    Datanode:
    @{lines} =          Split To Lines   ${datanodes}
    ${count} =          Get Length   ${lines}
    Should Be Equal As Integers    ${count}    ${expected}

*** Test Cases ***
List datanodes
                        Execute      ozone admin datanode list > datanode.list
    ${output} =         Get File     datanode.list
                        Should contain   ${output}   Datanode:
                        Should contain   ${output}   Related pipelines:

Filter list by UUID
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${output} =         Execute      ozone admin datanode list --id "${uuid}"
    Assert Output       ${output}    1    ${uuid}

Filter list by Ip address
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${ip} =             Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$3 }' | awk -F '[/]' '{ print \$3 }'
    ${output} =         Execute      ozone admin datanode list --ip "${ip}"
    Assert Output       ${output}    1    ${uuid}

Filter list by Hostname
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${hostname} =       Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$3 }' | awk -F '[/]' '{ print \$4 }'
    ${output} =         Execute      ozone admin datanode list --hostname "${hostname}"
    Assert Output       ${output}    1    ${uuid}

Filter list by NodeOperationalState
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${expected} =       Execute      grep -c 'Operational State: IN_SERVICE' datanode.list
    ${output} =         Execute      ozone admin datanode list --operational-state IN_SERVICE
    Assert Output       ${output}    ${expected}    ${uuid}

Filter list by NodeState
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${expected} =       Execute      grep -c 'Health State: HEALTHY' datanode.list
    ${output} =         Execute      ozone admin datanode list --node-state HEALTHY
    Assert Output       ${output}    ${expected}    ${uuid}

Get usage info by UUID
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${output} =         Execute      ozone admin datanode usageinfo --uuid "${uuid}"
    Should contain      ${output}    Usage Information (1 Datanodes)

Get usage info by Ip address
    ${ip} =             Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$3 }' | awk -F '[/]' '{ print \$3 }'
    ${output} =         Execute      ozone admin datanode usageinfo --address "${ip}"
    Should contain      ${output}    Usage Information (1 Datanodes)

Get usage info by Hostname
    ${hostname} =       Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$3 }' | awk -F '[/]' '{ print \$4 }'
    ${output} =         Execute      ozone admin datanode usageinfo --address "${hostname}"
    Should contain      ${output}    Usage Information (1 Datanodes)

Get usage info with invalid address
    ${uuid} =           Execute      grep '^Datanode:' datanode.list | head -1 | awk '{ print \$2 }'
    ${output} =         Execute      ozone admin datanode usageinfo --address "${uuid}"
    Should contain      ${output}    Usage Information (0 Datanodes)

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin datanode
                        Should contain   ${output}   Incomplete command
                        Should contain   ${output}   list

#List datanodes on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose datanode list --scm unknown-host
#                        Should contain   ${output}   Invalid host name

List datanodes as JSON
    ${output} =         Execute          ozone admin datanode list --json | jq -r '.'
                        Should contain   ${output}    datanodeDetails
                        Should contain   ${output}    healthState
                        Should contain   ${output}    opState

Get usage info as JSON
    ${output} =         Execute          ozone admin datanode usageinfo -m --json | jq -r '.'
                        Should contain   ${output}  capacity
                        Should contain   ${output}  committed
                        Should contain   ${output}  containerCount
                        Should contain   ${output}  datanodeDetails
                        Should contain   ${output}  freeSpaceToSpare
                        Should contain   ${output}  ozoneUsed
                        Should contain   ${output}  ozoneUsedPercent
                        Should contain   ${output}  remaining
                        Should contain   ${output}  remainingPercent
                        Should contain   ${output}  totalUsed
                        Should contain   ${output}  totalUsedPercent
