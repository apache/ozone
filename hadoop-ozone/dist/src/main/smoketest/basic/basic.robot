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
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${SCM}          scm


*** Test Cases ***

Check webui static resources
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit HTTP user
    ${result} =        Execute                curl --negotiate -u : -s -I http://${SCM}:9876/static/bootstrap-3.4.1/js/bootstrap.min.js
                       Should contain         ${result}    200

Start freon testing
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
    ${result} =        Execute              ozone freon randomkeys --num-of-volumes 5 --num-of-buckets 5 --num-of-keys 5 --num-of-threads 1 --replication-type RATIS --factor THREE --validate-writes
                       Wait Until Keyword Succeeds      3min       10sec     Should contain   ${result}   Number of Keys added: 125
                       Should Contain                   ${result}  Status: Success
