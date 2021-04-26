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
Documentation       Test ozone shell CLI usage
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ozone-shell-lib.robot
Test Setup          Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Kinit test user     testuser     testuser.keytab
Test Timeout        5 minute
Suite Setup         Generate prefix

*** Test Cases ***
RpcClient with port
   Test ozone shell       o3://            om:9862     ${prefix}-rpcwoport

RpcClient volume acls
   Test Volume Acls       o3://            om:9862     ${prefix}-rpcwoport2

RpcClient bucket acls
    Test Bucket Acls      o3://            om:9862     ${prefix}-rpcwoport2

RpcClient key acls
    Test Key Acls         o3://            om:9862     ${prefix}-rpcwoport2

RpcClient prefix acls
    Test Prefix Acls      o3://            om:9862     ${prefix}-rpcwoport2

RpcClient without host
    Test ozone shell      o3://            ${EMPTY}    ${prefix}-rpcwport

RpcClient without scheme
    Test ozone shell      ${EMPTY}         ${EMPTY}    ${prefix}-rpcwoscheme
