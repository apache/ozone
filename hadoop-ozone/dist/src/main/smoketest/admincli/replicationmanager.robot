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
Documentation       Test ozone admin replicationmanager command
Library             BuiltIn
Resource            ../commonlib.robot
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***
${SCM}       scm

*** Test Cases ***
Check replicationmanager
    ${output} =         Execute          ozone admin replicationmanager status
                        Should contain   ${output}   ReplicationManager
                        Should contain   ${output}   Running

Check replicationmanager with explicit host
    ${output} =         Execute          ozone admin replicationmanager status --scm ${SCM}
                        Should contain   ${output}   ReplicationManager
                        Should contain   ${output}   Running

Stop replicationmanager
    ${output} =         Execute          ozone admin replicationmanager stop
                        Should contain   ${output}   Stopping ReplicationManager
                        Wait Until Keyword Succeeds    30sec    5sec    Execute          ozone admin replicationmanager status | grep -q 'is Not Running'

Start replicationmanager
    ${output} =         Execute          ozone admin replicationmanager start
                        Should contain   ${output}   Starting ReplicationManager
                        Wait Until Keyword Succeeds    30sec    5sec    Execute          ozone admin replicationmanager status | grep -q 'is Running'

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin replicationmanager
                        Should contain   ${output}   Missing required subcommand
                        Should contain   ${output}   start
                        Should contain   ${output}   stop
                        Should contain   ${output}   status

#Check replicationmanager on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose replicationmanager status --scm unknown-host
#                        Should contain   ${output}   Invalid host name

