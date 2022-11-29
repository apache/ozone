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

*** Test Cases ***
Setup Cluster Data
    [Tags]  setup-ec-data
    Prepare Data For Xcompat Tests

Test Read Key Compat
    [Tags]  test-ec-compat
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}3mb      /tmp/3mb
    Key Should Match Local File     /${prefix}vol1/${prefix}default/${prefix}3mb    /tmp/3mb

    ${result} =     Execute and checkrc         ozone sh key get /${prefix}vol1/${prefix}ec/${prefix}3mb /tmp/${prefix}3mb       255
                    Should Contain  ${result}   NOT_SUPPORTED_OPERATION

Test Listing Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume list | jq -r '.name'
                    Should contain  ${result}   ${prefix}vol1
    ${result} =     Execute     ozone sh bucket list /${prefix}vol1/ | jq -r '.name'
                    Should contain  ${result}   ${prefix}default
                    Should contain  ${result}   ${prefix}ratis
                    Should contain  ${result}   ${prefix}ec
    ${result} =     Execute     ozone sh key list /${prefix}vol1/${prefix}default/ | jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'
                    Should contain  ${result}   ${prefix}3mb RATIS 3
    ${result} =     Execute     ozone sh key list /${prefix}vol1/${prefix}ratis/ | jq -r '[.name, .replicationType, (.replicationFactor | tostring)] | join (" ")'
                    Should contain  ${result}   ${prefix}3mb RATIS 3

    ${result} =     Execute and checkrc         ozone sh key list /${prefix}vol1/${prefix}ec/   255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION

Test Info Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume info ${prefix}vol1 | jq -r '.name'
                    Should contain  ${result}   ${prefix}vol1
    ${result} =     Execute     ozone sh bucket info /${prefix}vol1/${prefix}default | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   ${prefix}default        # there is no replication config in the old client for bucket info
    ${result} =     Execute     ozone sh bucket info /${prefix}vol1/${prefix}ratis | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   ${prefix}ratis        # there is no replication config in the old client for bucket info
    ${result} =     Execute     ozone sh bucket info /${prefix}vol1/${prefix}ec | jq -r '[.name, .replicationType, .replicationFactor] | join (" ")'
                    Should contain  ${result}   ${prefix}ec        # there is no replication config in the old client for bucket info

Test FS Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone fs -ls ofs://om/
                    Should contain  ${result}   /${prefix}vol1
    ${result} =     Execute     ozone fs -ls ofs://om/${prefix}vol1/
                    Should contain  ${result}   /${prefix}vol1/${prefix}default
                    Should contain  ${result}   /${prefix}vol1/${prefix}ratis
                    Should contain  ${result}   /${prefix}vol1/${prefix}ec
    ${result} =     Execute     ozone fs -ls ofs://om/${prefix}vol1/${prefix}default/${prefix}3mb
                    Should contain  ${result}   /${prefix}vol1/${prefix}default/${prefix}3mb
    ${result} =     Execute     ozone fs -ls ofs://om/${prefix}vol1/${prefix}ratis/${prefix}3mb
                    Should contain  ${result}   /${prefix}vol1/${prefix}ratis/${prefix}3mb

    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/${prefix}vol1/${prefix}ec/     1
                    Should contain  ${result}   ls: The list of keys contains keys with Erasure Coded replication set
    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/${prefix}vol1/${prefix}ec/${prefix}3mb     1
                    Should contain  ${result}   : No such file or directory
    ${result} =     Execute and checkrc    ozone fs -get ofs://om/${prefix}vol1/${prefix}ec/${prefix}3mb    1
                    Should contain  ${result}   : No such file or directory

Test FS Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone fs -put /tmp/1mb ofs://om/${prefix}vol1/${prefix}default/${prefix}1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/${prefix}vol1/${prefix}ratis/${prefix}1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/${prefix}vol1/${prefix}ec/${prefix}1mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}1mb      /tmp/1mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}1mb      /tmp/1mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}1mb      /tmp/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${prefix}vol1/${prefix}default/${prefix}1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${prefix}vol1/${prefix}ratis/${prefix}1mb
    Execute         ozone fs -rm -skipTrash ofs://om/${prefix}vol1/${prefix}ec/${prefix}1mb

Test Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone sh key put /${prefix}vol1/${prefix}default/${prefix}2mb /tmp/2mb
    Execute         ozone sh key put /${prefix}vol1/${prefix}ratis/${prefix}2mb /tmp/2mb
    Execute         ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}2mb /tmp/2mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}2mb      /tmp/2mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}2mb      /tmp/2mb
    Key Should Match Local File     /${prefix}vol1/${prefix}ratis/${prefix}2mb      /tmp/2mb
    Execute         ozone sh key delete /${prefix}vol1/${prefix}default/${prefix}2mb
    Execute         ozone sh key delete /${prefix}vol1/${prefix}ratis/${prefix}2mb
    Execute         ozone sh key delete /${prefix}vol1/${prefix}ec/${prefix}2mb
