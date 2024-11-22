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
Resource            ../ozone-lib/shell.robot

*** Variables ***
${SUFFIX}    ${EMPTY}

*** Test Cases ***
Setup Cluster Data
    [Tags]  setup-ec-data
    Prepare Data For Xcompat Tests

Test Read Key Compat
    [Tags]  test-ec-compat
    Key Should Match Local File     /vol1/ratis-${SUFFIX}/3mb      /tmp/3mb
    Key Should Match Local File     /vol1/default-${SUFFIX}/3mb    /tmp/3mb
    Assert Unsupported    ozone sh key get -f /vol1/ecbucket-${SUFFIX}/3mb /dev/null

Test Listing Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume list | jq -r '.name'
                    Should contain  ${result}   vol1
    ${result} =     Execute     ozone sh bucket list /vol1/ | jq -r '.name'
                    Should contain  ${result}   default
                    Should contain  ${result}   ratis
                    Should contain  ${result}   ec
    ${result} =     Key List With Replication    /vol1/default-${SUFFIX}/
                    Should contain  ${result}   3mb RATIS 3
    ${result} =     Key List With Replication    /vol1/ratis-${SUFFIX}/
                    Should contain  ${result}   3mb RATIS 3
    Assert Unsupported    ozone sh key list /vol1/ecbucket-${SUFFIX}/

Test Info Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone sh volume info vol1 | jq -r '.name'
                    Should contain  ${result}   vol1
    ${result} =     Bucket Replication    /vol1/default-${SUFFIX}
                    Should contain  ${result}   default        # there is no replication config in the old client for bucket info
    ${result} =     Bucket Replication    /vol1/ratis-${SUFFIX}
                    Should contain  ${result}   ratis        # there is no replication config in the old client for bucket info
    ${result} =     Bucket Replication    /vol1/ecbucket-${SUFFIX}
                    Should contain  ${result}   ec        # there is no replication config in the old client for bucket info

Test FS Compat
    [Tags]  test-ec-compat
    ${result} =     Execute     ozone fs -ls ofs://om/
                    Should contain  ${result}   /vol1
    ${result} =     Execute     ozone fs -ls ofs://om/vol1/
                    Should contain  ${result}   /vol1/default-${SUFFIX}
                    Should contain  ${result}   /vol1/ratis-${SUFFIX}
                    Should contain  ${result}   /vol1/ecbucket-${SUFFIX}
    ${result} =     Execute     ozone fs -ls ofs://om/vol1/default-${SUFFIX}/3mb
                    Should contain  ${result}   /vol1/default-${SUFFIX}/3mb
    ${result} =     Execute     ozone fs -ls ofs://om/vol1/ratis-${SUFFIX}/3mb
                    Should contain  ${result}   /vol1/ratis-${SUFFIX}/3mb

    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${SUFFIX}/     1
                    Should contain  ${result}   ls: The list of keys contains keys with Erasure Coded replication set
    ${result} =     Execute and checkrc    ozone fs -ls ofs://om/vol1/ecbucket-${SUFFIX}/3mb     1
                    Should contain  ${result}   : No such file or directory
    ${result} =     Execute and checkrc    ozone fs -get ofs://om/vol1/ecbucket-${SUFFIX}/3mb    1
                    Should contain  ${result}   : No such file or directory

Test FS Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone fs -put /tmp/1mb ofs://om/vol1/default-${SUFFIX}/1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/vol1/ratis-${SUFFIX}/1mb
    Execute         ozone fs -put /tmp/1mb ofs://om/vol1/ecbucket-${SUFFIX}/1mb
    Key Should Match Local File     /vol1/default-${SUFFIX}/1mb      /tmp/1mb
    Key Should Match Local File     /vol1/ratis-${SUFFIX}/1mb      /tmp/1mb
    Key Should Match Local File     /vol1/ecbucket-${SUFFIX}/1mb      /tmp/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/vol1/default-${SUFFIX}/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/vol1/ratis-${SUFFIX}/1mb
    Execute         ozone fs -rm -skipTrash ofs://om/vol1/ecbucket-${SUFFIX}/1mb

Test Client Can Read Own Writes
    [Tags]  test-ec-compat
    Execute         ozone sh key put /vol1/default-${SUFFIX}/2mb /tmp/2mb
    Execute         ozone sh key put /vol1/ratis-${SUFFIX}/2mb /tmp/2mb
    Execute         ozone sh key put /vol1/ecbucket-${SUFFIX}/2mb /tmp/2mb
    Key Should Match Local File     /vol1/default-${SUFFIX}/2mb      /tmp/2mb
    Key Should Match Local File     /vol1/ratis-${SUFFIX}/2mb      /tmp/2mb
    Key Should Match Local File     /vol1/ecbucket-${SUFFIX}/2mb      /tmp/2mb
    Execute         ozone sh key delete /vol1/default-${SUFFIX}/2mb
    Execute         ozone sh key delete /vol1/ratis-${SUFFIX}/2mb
    Execute         ozone sh key delete /vol1/ecbucket-${SUFFIX}/2mb
