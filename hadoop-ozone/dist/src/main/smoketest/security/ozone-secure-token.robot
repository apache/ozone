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
Documentation       Test token operations
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes
Suite Setup         Get Security Enabled From Config

*** Variables ***
${TOKEN_FILE}    ${TEMP_DIR}/ozone.token

*** Keywords ***
Get and use Token in Secure Cluster
    Run Keyword   Kinit test user     testuser     testuser.keytab
    Execute                      ozone sh token get -t ${TOKEN_FILE}
    File Should Not Be Empty     ${TOKEN_FILE}
    Execute                      kdestroy
    Set Environment Variable     HADOOP_TOKEN_FILE_LOCATION    ${TOKEN_FILE}
    ${output} =                  Execute             ozone sh volume list /
    Should not contain           ${output}           Client cannot authenticate
    Remove Environment Variable  HADOOP_TOKEN_FILE_LOCATION
    ${output} =                  Execute and Ignore Error  ozone sh volume list /
    Should contain               ${output}           Client cannot authenticate
    Run Keyword                  Kinit test user     testuser  testuser.keytab

Get Token in Unsecure Cluster
    ${output} =                  Execute             ozone sh token get -t ${TOKEN_FILE}
    Should Contain               ${output}           ozone sh token get
    Should Contain               ${output}           only when security is enabled

# should be executed after Get Token
Print Valid Token File
    ${output} =                  Execute             ozone sh token print -t ${TOKEN_FILE}
    Should Not Be Empty          ${output}

Print Nonexistent Token File
    ${output} =                  Execute             ozone sh token print -t /asdf
    Should Contain               ${output}           operation failed as token file: /asdf

Renew Token in Secure Cluster
    ${output} =                  Execute             ozone sh token renew -t ${TOKEN_FILE}
    Should contain               ${output}           Token renewed successfully

Renew Token in Unsecure Cluster
    ${output} =                  Execute             ozone sh token renew -t ${TOKEN_FILE}
    Should Contain               ${output}           ozone sh token renew
    Should Contain               ${output}           only when security is enabled

Cancel Token in Secure Cluster
    ${output} =                  Execute             ozone sh token cancel -t ${TOKEN_FILE}
    Should contain               ${output}           Token canceled successfully

Cancel Token in Unsecure Cluster
    ${output} =                  Execute             ozone sh token cancel -t ${TOKEN_FILE}
    Should Contain               ${output}           ozone sh token cancel
    Should Contain               ${output}           only when security is enabled

Token Test in Secure Cluster
    Get and use Token in Secure Cluster
    Print Valid Token File
    Renew Token in Secure Cluster
    Cancel Token in Secure Cluster

Token Test in Unsecure Cluster
    Get Token in Unsecure Cluster
    Renew Token in Unsecure Cluster
    Cancel Token in Unsecure Cluster

*** Test Cases ***
Token Test
    Run Keyword if    '${SECURITY_ENABLED}' == 'false'   Token Test in Unsecure Cluster
    Run Keyword if    '${SECURITY_ENABLED}' == 'true'    Token Test in Secure Cluster
    Print Nonexistent Token File
