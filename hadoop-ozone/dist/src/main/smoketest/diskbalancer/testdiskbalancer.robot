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
Documentation       Test ozone admin datanode diskbalancer command
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot

*** Variables ***
${LIST_FILE}    /tmp/datanode.list
${STDIN_FILE}   /tmp/datanode.stdin

*** Keywords ***
Get Datanode Address
    [arguments]    ${index}=1
    ${hostname} =    Execute    grep '^Datanode:' ${LIST_FILE} | sed -n '${index}p' | awk '{ print \$3 }' | awk -F '[/]' '{ print \$4 }' | awk -F '[.]' '{ print \$1 }'
    [return]    ${hostname}

Setup Datanode List
    Execute    ozone admin datanode list > ${LIST_FILE}
    ${output} =    Get File    ${LIST_FILE}
    Should contain    ${output}    Datanode:

*** Test Cases ***
Setup
    Setup Datanode List

Check failure with non-admin user to start, stop and update diskbalancer with --in-service-datanodes
    Requires admin privilege     ozone admin datanode diskbalancer start --in-service-datanodes
    Requires admin privilege     ozone admin datanode diskbalancer stop --in-service-datanodes
    Requires admin privilege     ozone admin datanode diskbalancer update -t 0.0002 --in-service-datanodes

Check success with admin user for start, stop and update diskbalancer with --in-service-datanodes
    Run Keyword         Kinit test user                 testuser                testuser.keytab
    ${result} =         Execute                         ozone admin datanode diskbalancer start --in-service-datanodes
                        Should Contain                  ${result}                Started DiskBalancer on all IN_SERVICE nodes.
    ${result} =         Execute                         ozone admin datanode diskbalancer stop --in-service-datanodes
                        Should Contain                  ${result}                Stopped DiskBalancer on all IN_SERVICE nodes.
    ${result} =         Execute                         ozone admin datanode diskbalancer update -t 0.0002 --in-service-datanodes
                        Should Contain                  ${result}                Updated DiskBalancer configuration on all IN_SERVICE nodes.

Check success with non-admin user for status and report diskbalancer with --in-service-datanodes
    Run Keyword         Kinit test user                 testuser2               testuser2.keytab
    ${result} =         Execute                         ozone admin datanode diskbalancer status --in-service-datanodes
                        Should Contain                  ${result}                Status result:
    ${result} =         Execute                         ozone admin datanode diskbalancer report --in-service-datanodes
                        Should Contain                  ${result}                Report result:

Start, stop and update diskbalancer on specific datanode
    Run Keyword         Kinit test user                 testuser                testuser.keytab
    ${dn1} =            Get Datanode Address    1
    ${result} =         Execute                         ozone admin datanode diskbalancer start ${dn1}
                        Should Contain                  ${result}                Started DiskBalancer on nodes:
                        Should Contain                  ${result}                ${dn1}
    ${result} =         Execute                         ozone admin datanode diskbalancer stop ${dn1}
                        Should Contain                  ${result}                Stopped DiskBalancer on nodes:
                        Should Contain                  ${result}                ${dn1}
    ${result} =         Execute                         ozone admin datanode diskbalancer update -t 0.0001 -b 100 ${dn1}
                        Should Contain                  ${result}                Updated DiskBalancer configuration on nodes:
                        Should Contain                  ${result}                ${dn1}

 Start, stop and update diskbalancer using stdin
    Run Keyword         Kinit test user                 testuser                testuser.keytab
    ${dn1} =            Get Datanode Address    1
    ${dn2} =            Get Datanode Address    2
    Create File         ${STDIN_FILE}                   ${dn1}\n${dn2}
    ${result} =         Execute                         cat ${STDIN_FILE} | ozone admin datanode diskbalancer start -
                        Should Contain                  ${result}                Started DiskBalancer on nodes:
                        Should Contain                  ${result}                ${dn1}
                        Should Contain                  ${result}                ${dn2}
    ${result} =         Execute                         cat ${STDIN_FILE} | ozone admin datanode diskbalancer stop -
                        Should Contain                  ${result}                Stopped DiskBalancer on nodes:
                        Should Contain                  ${result}                ${dn1}
                        Should Contain                  ${result}                ${dn2}
    ${result} =         Execute                         cat ${STDIN_FILE} | ozone admin datanode diskbalancer update -t 0.0001 -b 100 -
                        Should Contain                  ${result}                Updated DiskBalancer configuration on nodes:
                        Should Contain                  ${result}                ${dn1}
                        Should Contain                  ${result}                ${dn2}

Get status and report of diskbalancer using stdin
    Run Keyword         Kinit test user                 testuser2               testuser2.keytab
    ${dn1} =            Get Datanode Address    1
    ${dn2} =            Get Datanode Address    2
    Create File         ${STDIN_FILE}                   ${dn1}\n${dn2}
    ${result} =         Execute                         cat ${STDIN_FILE} | ozone admin datanode diskbalancer status -
                        Should Contain                  ${result}                Status result:
                        Should Contain                  ${result}                ${dn1}
                        Should Contain                  ${result}                ${dn2}
    ${result} =         Execute                         cat ${STDIN_FILE} | ozone admin datanode diskbalancer report -
                        Should Contain                  ${result}                Report result:
                        Should Contain                  ${result}                ${dn1}
                        Should Contain                  ${result}                ${dn2}

Get status of diskbalancer with --json option
    Run Keyword         Kinit test user                 testuser2               testuser2.keytab
    ${output} =         Execute                         ozone admin datanode diskbalancer status --json --in-service-datanodes | jq -r '.'
                        Should contain                  ${output}                datanode
                        Should contain                  ${output}                status
                        Should contain                  ${output}                threshold
                        Should contain                  ${output}                bandwidthInMB
                        Should contain                  ${output}                threads
                        Should contain                  ${output}                successMove
                        Should contain                  ${output}                failureMove
                        Should contain                  ${output}                bytesMovedMB
                        Should contain                  ${output}                estBytesToMoveMB
                        Should contain                  ${output}                estTimeLeftMin
    ${array_length} =   Execute                         ozone admin datanode diskbalancer status --json --in-service-datanodes | jq 'length'
                        Should Be True                  ${array_length} > 0

Get report of diskbalancer with --json option
    ${output} =         Execute                         ozone admin datanode diskbalancer report --json --in-service-datanodes | jq -r '.'
                        Should contain                  ${output}                datanode
                        Should contain                  ${output}                volumeDensity
    ${array_length} =   Execute                         ozone admin datanode diskbalancer report --json --in-service-datanodes | jq 'length'
                        Should Be True                  ${array_length} > 0
