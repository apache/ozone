
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
Documentation       Smoke test for spnego with docker-compose environments.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${SCM}              scm

${OM_URL}           http://om:9874
${RECON_URL}        http://recon:9888
${S3G_URL}          http://s3g:19878
${SCM_URL}          http://${SCM}:9876

@{BASE_URLS}        ${OM_URL}    ${RECON_URL}    ${S3G_URL}    ${SCM_URL}
@{COMMON_PATHS}     conf   jmx    logLevel    logs/    prom    stacks
@{CUSTOM_ENDPOINTS}    ${OM_URL}/dbCheckpoint    ${OM_URL}/serviceList    ${SCM_URL}/dbCheckpoint


*** Keywords ***
Verify SPNEGO URL
    [arguments]         ${url}        ${expected_response}
    ${result} =         Execute       curl --negotiate -u : --silent -o /dev/null -w '\%{http_code}' '${url}'
    IF    '${result}' != '${expected_response}'
        # repeat with verbose mode for debug
        Execute       curl --negotiate -u : -vvv -o /dev/null '${url}'
        Should Be Equal    '${result}'    '${expected_response}'
    END

*** Test Cases ***
Verify SPNEGO URLs without auth
    [setup]        Execute    kdestroy
    [template]     Verify SPNEGO URL

    FOR    ${BASE_URL}    IN    @{BASE_URLS}
      FOR    ${PATH}    IN    @{COMMON_PATHS}
          ${BASE_URL}/${PATH}    401
      END
    END
    FOR    ${URL}    IN    @{CUSTOM_ENDPOINTS}
        ${URL}    401
    END

Verify SPNEGO URLs with auth
    [setup]    Kinit test user     testuser    testuser.keytab
    [template]     Verify SPNEGO URL

    FOR    ${BASE_URL}    IN    @{BASE_URLS}
      FOR    ${PATH}    IN    @{COMMON_PATHS}
          ${BASE_URL}/${PATH}    200
      END
    END
    FOR    ${URL}    IN    @{CUSTOM_ENDPOINTS}
        ${URL}    200
    END
