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
Documentation       Test EC backward compatibility
Library             OperatingSystem
Resource            lib.resource

*** Variables ***
${PREFIX}    ${EMPTY}
${VOLUME}    vol${PREFIX}

*** Test Cases ***
Setup Cluster Data
    [Tags]  setup-ec-data
    Prepare Data For Xcompat Tests

Test Read Key Compat
    [Tags]  test-ec-compat
    Key Should Match Local File     /${VOLUME}/ratis/3mb      /tmp/3mb
    Key Should Match Local File     /${VOLUME}/default/3mb    /tmp/3mb

    ${result} =     Execute and checkrc         ozone sh key get -f /${VOLUME}/ecbucket/3mb /dev/null       255
                    Should Contain  ${result}   NOT_SUPPORTED_OPERATION

Test Listing Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume list | jq -r '.name'
                    Should contain  ${result}   ${VOLUME}
    ${result} =     Execute     ozone sh bucket list /${VOLUME}/ | jq -r '.name'
                    Should contain  ${result}   default
                    Should contain  ${result}   ratis
                    Should contain  ${result}   ec
    ${result} =     Execute     ozone sh key list /${VOLUME}/default/ | jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'
                    Should contain  ${result}   3mb RATIS 3
    ${result} =     Execute     ozone sh key list /${VOLUME}/ratis/ | jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'
                    Should contain  ${result}   3mb RATIS 3

    ${result} =     Execute and checkrc         ozone sh key list /${VOLUME}/ecbucket/   255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION

Test Info Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume info ${VOLUME} | jq -r '.name'
                    Should contain  ${result}   ${VOLUME}
    ${result} =     Execute     ozone sh bucket info /${VOLUME}/default | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   default        # there is no replication config in the old client for bucket info
    ${result} =     Execute     ozone sh bucket info /${VOLUME}/ratis | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   ratis        # there is no replication config in the old client for bucket info
    ${result} =     Execute     ozone sh bucket info /${VOLUME}/ecbucket | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   ec        # there is no replication config in the old client for bucket info

Test FS Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone fs -ls ofs://om/
                    Should contain  ${result}   /${VOLUME}
    ${result} =     Execute     ozone fs -ls ofs://om/${VOLUME}/
                    Should contain  ${result}   /${VOLUME}/default
                    Should contain  ${result}   /${VOLUME}/ratis
                    Should contain  ${result}   /${VOLUME}/ecbucket
    ${result} =     Execute     ozone fs -ls ofs://om/${VOLUME}/default/3mb
                    Should contain  ${result}   /${VOLUME}/default/3mb
    ${result} =     Execute     ozone fs -ls ofs://om/${VOLUME}/ratis/3mb
                    Should contain  ${result}   /${VOLUME}/ratis/3mb

    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/${VOLUME}/ecbucket/     1
                    Should contain  ${result}   ls: The list of keys contains keys with Erasure Coded replication set
    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/${VOLUME}/ecbucket/3mb     1
                    Should contain  ${result}   : No such file or directory
    ${result} =     Execute and checkrc    ozone fs -get ofs://om/${VOLUME}/ecbucket/3mb    1
                    Should contain  ${result}   : No such file or directory

Test FS Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone fs -put /tmp/1mb ofs://om/${VOLUME}/default/1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/${VOLUME}/ratis/1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/${VOLUME}/ecbucket/1mb
    Key Should Match Local File     /${VOLUME}/ratis/1mb      /tmp/1mb
    Key Should Match Local File     /${VOLUME}/ratis/1mb      /tmp/1mb
    Key Should Match Local File     /${VOLUME}/ratis/1mb      /tmp/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${VOLUME}/default/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${VOLUME}/ratis/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${VOLUME}/ecbucket/1mb

Test Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone sh key put /${VOLUME}/default/2mb /tmp/2mb
    Execute         ozone sh key put /${VOLUME}/ratis/2mb /tmp/2mb
    Execute         ozone sh key put /${VOLUME}/ecbucket/2mb /tmp/2mb
    Key Should Match Local File     /${VOLUME}/ratis/2mb      /tmp/2mb
    Key Should Match Local File     /${VOLUME}/ratis/2mb      /tmp/2mb
    Key Should Match Local File     /${VOLUME}/ratis/2mb      /tmp/2mb
    Execute         ozone sh key delete /${VOLUME}/default/2mb
    Execute         ozone sh key delete /${VOLUME}/ratis/2mb
    Execute         ozone sh key delete /${VOLUME}/ecbucket/2mb
