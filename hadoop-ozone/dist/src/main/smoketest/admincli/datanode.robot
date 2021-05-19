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
Test Timeout        5 minutes

*** Test Cases ***
List datanodes
    ${output} =         Execute          ozone admin datanode list
                        Should contain   ${output}   Datanode:
                        Should contain   ${output}   Related pipelines:

Filter list by UUID
    ${uuid} =           Execute      ozone admin datanode list | grep '^Datanode:' | head -1 | awk '{ print \$2 }'
    ${output} =         Execute      ozone admin datanode list --id "${uuid}"
    Should contain      ${output}    Datanode: ${uuid}
    ${datanodes} =      Get Lines Containing String    ${output}    Datanode:
    @{lines} =          Split To Lines   ${datanodes}
    ${count} =          Get Length   ${lines}
    Should Be Equal As Integers    ${count}    1

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin datanode
                        Should contain   ${output}   Incomplete command
                        Should contain   ${output}   list

#List datanodes on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose datanode list --scm unknown-host
#                        Should contain   ${output}   Invalid host name
