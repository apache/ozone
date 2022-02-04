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
Suite Setup         Prepare For Tests

*** Variables ***
${SCM}       scm

*** Keywords ***

Prepare For Tests
    ${random} =         Generate Random String  5  [NUMBERS]
    Set Suite Variable  ${prefix}  ${random}
    Execute             dd if=/dev/urandom of=/tmp/1mb bs=1048576 count=1
    Execute             dd if=/dev/urandom of=/tmp/2mb bs=1048576 count=2
    Execute             dd if=/dev/urandom of=/tmp/3mb bs=1048576 count=3
    Execute             dd if=/dev/urandom of=/tmp/100mb bs=1048576 count=100

*** Test Cases ***

Test Bucket Creation
    ${result} =     Execute             ozone sh volume create /${prefix}vol1
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket create /${prefix}vol1/${prefix}ratis
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket list /${prefix}vol1 | jq -r '.[] | select(.name | contains("${prefix}ratis")) | .replicationConfig.replicationType'
                    Should contain      ${result}       RATIS
    ${result} =     Execute             ozone sh bucket create --replication rs-3-2-1024k --type EC /${prefix}vol1/${prefix}ec
                    Should not contain  ${result}       Failed
    ${result} =     Execute             ozone sh bucket list /${prefix}vol1 | jq -r '.[] | select(.name | contains("${prefix}ec")) | .replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$

Test key Creation
                    Execute                       ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}1mb /tmp/1mb
                    Execute                       ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}2mb /tmp/2mb
                    Execute                       ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}3mb /tmp/3mb
                    Execute                       ozone sh key put /${prefix}vol1/${prefix}ec/${prefix}100mb /tmp/100mb

                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}1mb      /tmp/1mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}2mb      /tmp/2mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}3mb      /tmp/3mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}100mb    /tmp/100mb

    # Check one key has the correct replication details
    ${result}       Execute                       ozone sh key info /${prefix}vol1/${prefix}ec/${prefix}1mb | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$

Test Ratis Key EC Bucket
                    Execute                       ozone sh key put --replication=THREE --type=RATIS /${prefix}vol1/${prefix}ec/${prefix}1mbRatis /tmp/1mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ec/${prefix}1mbRatis    /tmp/1mb
    ${result}       Execute                       ozone sh key info /${prefix}vol1/${prefix}ec/${prefix}1mbRatis | jq -r '.replicationConfig.replicationType'
                    Should Match Regexp           ${result}       ^(?m)RATIS$

Test EC Key Ratis Bucket
                    Execute                       ozone sh key put --replication=rs-3-2-1024k --type=EC /${prefix}vol1/${prefix}ratis/${prefix}1mbEC /tmp/1mb
                    Key Should Match Local File   /${prefix}vol1/${prefix}ratis/${prefix}1mbEC    /tmp/1mb
    ${result}       Execute                       ozone sh key info /${prefix}vol1/${prefix}ratis/${prefix}1mbEC | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                    Should Match Regexp      ${result}       ^(?m)EC$
                    Should Match Regexp      ${result}       ^(?m)RS$
                    Should Match Regexp      ${result}       ^(?m)3$
                    Should Match Regexp      ${result}       ^(?m)2$
                    Should Match Regexp      ${result}       ^(?m)1048576$
