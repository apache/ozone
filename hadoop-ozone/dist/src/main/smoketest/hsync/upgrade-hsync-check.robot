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
Documentation       Test HSync during upgrade
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../lib/fs.robot
Resource            ../debug/ozone-debug.robot
Default Tags        pre-finalized-hsync-tests
Suite Setup         Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab

*** Variables ***
${OM_SERVICE_ID}    %{OM_SERVICE_ID}
${VOLUME}           upgrade-hsync-volume
${BUCKET}           upgrade-hsync-bucket
${KEY}              upgrade-hsync-key

*** Keywords ***
Create volume bucket and put key
    Execute             ozone sh volume create /${volume}
    Execute             ozone sh bucket create /${volume}/${bucket}
    Execute             ozone sh key put /${volume}/${bucket}/${key} /etc/hosts

*** Test Cases ***
Test HSync Prior To Finalization
    Create volume bucket and put key
    ${o3fs_path} =  Format FS URL          o3fs     ${VOLUME}    ${BUCKET}    ${KEY}
    ${result} =     Execute and checkrc    ozone debug recover --path=${o3fs_path}    255
                    Should contain  ${result}  It belongs to the layout feature HBASE_SUPPORT, whose layout version is 7
    ${ofs_path} =   Format FS URL          ofs      ${VOLUME}    ${BUCKET}    ${KEY}
    ${result} =     Execute and checkrc    ozone debug recover --path=${ofs_path}    255
                    Should contain  ${result}  It belongs to the layout feature HBASE_SUPPORT, whose layout version is 7
