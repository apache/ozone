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
Documentation       Test ozone admin command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config

*** Keywords ***
Setup Test
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Test Cases ***
Reconfigure OM
    Pass Execution If       '${SECURITY_ENABLED}' == 'false'    N/A
        ${output} =             Execute          ozone admin reconfig --address=om:9862 --service=OM start
        Should Contain          ${output}        Started reconfiguration task on node
Reconfigure SCM
    Pass Execution If       '${SECURITY_ENABLED}' == 'false'    N/A
        ${output} =             Execute          ozone admin reconfig --address=scm:9860 --service=SCM start
        Should Contain          ${output}        Started reconfiguration task on node
Reconfigure DN
    Pass Execution If       '${SECURITY_ENABLED}' == 'false'    N/A
        ${output} =             Execute          ozone admin reconfig --address=datanode:19864 --service=DATANODE start
        Should Contain          ${output}        Started reconfiguration task on node
