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
Documentation       Test ozone admin container command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Create test data

*** Variables ***
${CONTAINER}
${SCM}       scm

*** Keywords ***
Create test data
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
                        Execute          ozone freon ockg -n1 -t1 -p container

Container is closed
    [arguments]     ${container}
    ${output} =         Execute          ozone admin container info "${container}"
                        Should contain   ${output}   CLOSED

*** Test Cases ***
Create container
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    ${container} =      Execute          echo "${output}" | grep 'is created' | cut -f2 -d' '
                        Set Suite Variable    ${CONTAINER}    ${container}

List containers
    ${output} =         Execute          ozone admin container list
                        Should contain   ${output}   OPEN

List containers with explicit host
    ${output} =         Execute          ozone admin container list --scm ${SCM}
                        Should contain   ${output}   OPEN

List containers with container state
    ${output} =         Execute          ozone admin container list --state=CLOSED
                        Should Not contain   ${output}   OPEN

List containers with replication factor ONE
    ${output} =         Execute          ozone admin container list -t RATIS -r ONE
                        Should Not contain   ${output}   THREE

List containers with replication factor THREE
    ${output} =         Execute          ozone admin container list -t RATIS -r THREE
                        Should Not contain   ${output}   ONE

Container info
    ${output} =         Execute          ozone admin container info "${CONTAINER}"
                        Should contain   ${output}   Container id: ${CONTAINER}
                        Should contain   ${output}   Pipeline id
                        Should contain   ${output}   Datanodes

Verbose container info
    ${output} =         Execute          ozone admin --verbose container info "${CONTAINER}"
                        Should contain   ${output}   Pipeline Info

Close container
    ${container} =      Execute          ozone admin container list --state OPEN | jq -r 'select(.replicationConfig.replicationFactor == "THREE") | .containerID' | head -1
                        Execute          ozone admin container close "${container}"
    ${output} =         Execute          ozone admin container info "${container}"
                        Should contain   ${output}   CLOS
    Wait until keyword succeeds    1min    10sec    Container is closed    ${container}

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin container
                        Should contain   ${output}   Incomplete command
                        Should contain   ${output}   list
                        Should contain   ${output}   info
                        Should contain   ${output}   create
                        Should contain   ${output}   close
                        Should contain   ${output}   reconcile

#List containers on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose container list --scm unknown-host
#                        Should contain   ${output}   Invalid host name

Cannot close container without admin privilege
    Requires admin privilege    ozone admin container close "${CONTAINER}"

Cannot create container without admin privilege
    Requires admin privilege    ozone admin container create

Cannot reconcile container without admin privilege
    Requires admin privilege    ozone admin container reconcile "${CONTAINER}"

Reset user
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Cannot reconcile open container
    ${output} =         Execute          ozone admin container create
                        Should contain   ${output}   is created
    # The newly created container should still be open.
    ${container} =      Execute          ozone admin container list --state OPEN | jq -r 'select(.replicationConfig.replicationFactor == "THREE") | .containerID' | head -n1
    Execute and check rc    ozone admin container reconcile "${container}"    255

Reconcile closed container
    # Get a currently open container to use for this test.
    ${container} =      Execute          ozone admin container list --state OPEN | jq -r 'select(.replicationConfig.replicationFactor == "THREE") | .containerID' | head -n1
    # The container should not yet have any replica checksums.
    ${data_checksum} =  Execute          ozone admin container info "${container}" --json | jq -r '.replicas[].dataChecksum' | head -n1
    Should be empty    ${data_checksum}
    # Close the container to it can be reconciled.
    Execute    ozone admin container close ${container}
    # Check info still does not show replica checksums
    ${data_checksum} =  Execute          ozone admin container info "${container}" --json | jq -r '.replicas[].dataChecksum' | head -n1
    Should be empty    ${data_checksum}
    # Reconcile, and checksums should show up.
    Execute    ozone admin container reconcile ${container}
    # When reconciliation finishes, replica checksums should be shown.
    ${data_checksum} =  Execute          ozone admin container info "${container}" --json | jq -r '.replicas[].dataChecksum' | head -n1
     Wait until keyword succeeds    1min    5sec    Should not be empty    ${data_checksum}
