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
Documentation       Test EC shell commands
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../ozone-lib/shell.robot
Resource            lib.resource
Suite Setup         Prepare For Tests

*** Test Cases ***
Test Bucket Creation
    ${result} =     Execute             ozone sh volume create /${prefix}vol1
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${prefix}vol1/${prefix}default
                    Should not contain  ${result}       Failed
                    Verify Bucket Empty Replication Config      /${prefix}vol1/${prefix}default
    ${result} =     Execute             ozone sh bucket create --replication 3 --type RATIS /${prefix}vol1/${prefix}ratis
                    Should not contain  ${result}       Failed
                    Verify Bucket Replica Replication Config    /${prefix}vol1/${prefix}ratis   RATIS   THREE
    ${result} =     Execute             ozone sh bucket create --replication rs-3-2-1024k --type EC /${prefix}vol1/${prefix}ec
                    Should not contain  ${result}       Failed
                    Verify Bucket EC Replication Config    /${prefix}vol1/${prefix}ec    RS    3    2    1048576

Test Key Creation EC Bucket
                    Execute                             ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}1mb /tmp/1mb
                    Execute                             ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}2mb /tmp/2mb
                    Execute                             ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}3mb /tmp/3mb
                    Execute                             ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}100mb /tmp/100mb

                    Key Should Match Local File         /${prefix}vol1/${prefix}ec/${prefix}1mb      /tmp/1mb
                    Key Should Match Local File         /${prefix}vol1/${prefix}ec/${prefix}2mb      /tmp/2mb
                    Key Should Match Local File         /${prefix}vol1/${prefix}ec/${prefix}3mb      /tmp/3mb
                    Key Should Match Local File         /${prefix}vol1/${prefix}ec/${prefix}100mb    /tmp/100mb

                    Verify Key EC Replication Config    /${prefix}vol1/${prefix}ec/${prefix}1mb    RS    3    2    1048576

Test Key Creation Default Bucket
                    Execute                             ozone sh key put /${prefix}vol1/${prefix}default/${prefix}1mb /tmp/1mb
                    Key Should Match Local File         /${prefix}vol1/${prefix}default/${prefix}1mb      /tmp/1mb
                    Verify Key Replica Replication Config   /${prefix}vol1/${prefix}default/${prefix}1mb     RATIS    THREE

Test Ratis Key EC Bucket
                    Execute                       ozone sh key put --replication=THREE --type=RATIS /${prefix}vol1/${prefix}ec/${prefix}1mbRatis /tmp/1mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}1mbRatis    /tmp/1mb
                    Verify Key Replica Replication Config   /${prefix}vol1/${prefix}ec/${prefix}1mbRatis    RATIS   THREE

Test EC Key Ratis Bucket
                    Execute                             ozone sh key put --replication=rs-3-2-1024k --type=EC /${prefix}vol1/${prefix}ratis/${prefix}1mbEC /tmp/1mb
                    Key Should Match Local File         /${prefix}vol1/${prefix}ratis/${prefix}1mbEC    /tmp/1mb
                    Verify Key EC Replication Config    /${prefix}vol1/${prefix}ratis/${prefix}1mbEC    RS    3    2    1048576