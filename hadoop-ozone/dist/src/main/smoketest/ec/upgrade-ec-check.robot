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
Documentation       Test EC during upgrade
Library             OperatingSystem
Resource            lib.resource
Suite Setup         Kinit test user     testuser     testuser.keytab

*** Test Cases ***
Test EC Prior To Finalization
    [Tags]  pre-finalized-ec-tests
    Execute         ozone sh volume create /ectest
    ${result} =     Execute and checkrc     ozone sh bucket create --replication rs-3-2-1024k --type EC /ectest/ectest     255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION
    Execute         ozone sh bucket create /ectest/testpropchange
    ${result} =     Execute and checkrc     ozone sh bucket set-replication-config -r rs-3-2-1024k -t EC /ectest/testpropchange     255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION
    ${result} =     Execute and checkrc     ozone sh key put -r rs-3-2-1024k -t EC /ectest/testpropchange/core-site.xml /etc/hadoop/core-site.xml     255
                    Should contain  ${result}   NOT_SUPPORTED_OPERATION



Test EC After Finalization
    [Tags]  post-finalized-ec-tests
    Execute         ozone sh volume create /ectest-new
    Execute         ozone sh bucket create --replication rs-3-2-1024k --type EC /ectest-new/ectest
                    Verify Bucket EC Replication Config     /ectest-new/ectest  RS  3   2   1048576
    Execute         ozone sh bucket create /ectest-new/testpropchange
    Execute         ozone sh bucket set-replication-config -r rs-3-2-1024k -t EC /ectest-new/testpropchange
                    Verify Bucket EC Replication Config     /ectest-new/testpropchange  RS  3   2   1048576
    Execute         ozone sh key put -r rs-3-2-1024k -t EC /ectest-new/ectest/core-site.xml /etc/hadoop/core-site.xml
                    Key Should Match Local File     /ectest-new/ectest/core-site.xml        /etc/hadoop/core-site.xml
                    Verify Key EC Replication Config    /ectest-new/ectest/core-site.xml    RS  3   2   1048576
