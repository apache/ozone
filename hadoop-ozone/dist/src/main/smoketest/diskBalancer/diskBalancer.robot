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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             String
Library             Collections
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot

** Test Cases ***
Check admin permission to start diskbalancer
    Requires admin privilege     ozone admin datanode diskbalancer start -a
    # Test user has admin privileges, hence command should be executed successfully
    Run Keyword         Kinit test user
    ${result} =         Execute and Ignore Error        ozone admin datanode diskbalancer start -t 0.0002 -a
                        Should Contain                  ${result}             Start DiskBalancer on datanode(s)

Check admin permission to stop diskbalancer
    Requires admin privilege     ozone admin datanode diskbalancer stop -a
    # Test user has admin privileges, hence command should be executed successfully
    Run Keyword         Kinit test user
    ${result} =         Execute and Ignore Error        ozone admin datanode diskbalancer stop -a
                        Should Contain                  ${result}             Stopping DiskBalancer on datanode(s)

Check admin permission to update diskbalancer configuration
    Requires admin privilege     ozone admin datanode diskbalancer update -t 0.0002 -a
    # Test user has admin privileges, hence command should be executed successfully
    Run Keyword         Kinit test user
    ${result} =         Execute and Ignore Error        ozone admin datanode diskbalancer update -t 0.0002 -a
                        Should Contain                  ${result}             Update DiskBalancer Configuration on datanode(s)

Check with non admin user is it able to read diskbalancer status
    # Test user2 has no admin privileges but still command should be executed successfully
    Run Keyword         Kinit test user             testuser2               testuser2.keytab
    ${result} =         Execute                         ozone admin datanode diskbalancer status
                        Should Contain                  ${result}             Status result:

Check with non admin user is it able to read diskbalancer report
    # Test user2 has no admin privileges but still command should be executed successfully
    Run Keyword         Kinit test user             testuser2               testuser2.keytab
    ${result} =         Execute                         ozone admin datanode diskbalancer report
                        Should Contain                  ${result}             Report result:




