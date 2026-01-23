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
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config

*** Variables ***
${SCM}          scm


*** Test Cases ***

Check webui static resources
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit HTTP user
    ${result} =        Execute                curl --negotiate -u : -s -I http://${SCM}:9876/static/bootstrap-3.4.1/js/bootstrap.min.js
                       Should contain         ${result}    200

Basic Freon smoketest
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
    ${random} =        Generate Random String    10
    Freon OCKG    prefix=${random}
    Freon OCKV    prefix=${random}
