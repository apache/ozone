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
Documentation       S3 gateway test with aws cli
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            ../s3/commonawslib.robot
Suite Setup         Get Security Enabled From Config

*** Variables ***
${OM_JMX_ENDPOINT}                          http://om:9874/jmx
${S3_ENDPOINT_URL}                          http://s3g:9878
${OM_PORT}                                  9862
${IPC_NAMESPACE}                            ipc.${OM_PORT}
${DECAY_RPC_SCHEDULER_METRICS_NAME}         DecayRpcSchedulerMetrics2.${IPC_NAMESPACE}

*** Keywords ***
Setup headers
    # Stores the user to a suite variable, TEST_USER
    # Use TEST_USER to verify the caller from the metrics
    # This is run under s3g, so the user should be `testuser/s3g@EXAMPLE.COM`
    Kinit test user    testuser     testuser.keytab
    Setup secure v4 headers

Setup aws credentials
    ${accessKey} =      Execute     aws configure get aws_access_key_id
    ${secret} =         Execute     aws configure get aws_secret_access_key
    Set Environment Variable        AWS_SECRET_ACCESS_KEY  ${secret}
    Set Environment Variable        AWS_ACCESS_KEY_ID  ${accessKey}

Verify endpoint is up
    [arguments]         ${url}
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -v -s -I ${url}
    Should contain      ${result}       200 OK

Setup bucket1
    Execute AWSS3APICli             create-bucket --bucket bucket1

Freon s3kg
    [arguments]         ${prefix}=s3bg      ${n}=1000       ${threads}=10       ${args}=${EMPTY}
    ${result} =         Execute             ozone freon s3kg -e ${S3_ENDPOINT_URL} -t ${threads} -n ${n} -p ${prefix} ${args}
                        Should contain      ${result}       Successful executions: ${n}

Check metrics caller
    ${result} =         Execute             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${DECAY_RPC_SCHEDULER_METRICS_NAME}/,/}/p' | grep 'Caller('
                        Should contain      ${result}       ${TEST_USER}

*** Test Cases ***
Test setup headers
    Setup headers

Test setup credentials
    Setup aws credentials

Test create bucket 1
    Setup bucket1

Test OM JMX endpoint
    Verify endpoint is up       ${OM_JMX_ENDPOINT}

Test metrics are registered
    ${result} =         Execute                             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | grep ${DECAY_RPC_SCHEDULER_METRICS_NAME}
                        Should contain      ${result}       ${DECAY_RPC_SCHEDULER_METRICS_NAME}

Run freon s3kg
    Freon s3kg

Test caller from metrics
    Check metrics caller
