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
Documentation       Keywords and other resources used by EC tests
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Suite Setup         Get Security Enabled From Config

*** Variables ***
${SCM}       scm

*** Keywords ***
Prepare For Tests
    Execute             dd if=/dev/urandom of=/tmp/1mb bs=1048576 count=1
    Execute             dd if=/dev/urandom of=/tmp/2mb bs=1048576 count=2
    Execute             dd if=/dev/urandom of=/tmp/3mb bs=1048576 count=3
    Execute             dd if=/dev/urandom of=/tmp/100mb bs=1048576 count=100
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user    testuser    testuser.keytab
