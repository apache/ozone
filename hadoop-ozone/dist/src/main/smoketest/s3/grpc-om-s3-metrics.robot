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
Documentation       gRPC Om S3 gateway metrics test
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot
Resource            commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${GRPC_ENABLED}             false
${OM_URL}                   http://om:9874
${OM_JMX_ENDPOINT}          ${OM_URL}/jmx
${GRPC_OM_METRICS_NAME}     GrpcOzoneManagerMetrics

*** Keywords ***
Verify endpoint is up
    [arguments]         ${url}
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
    ${result} =         Execute                             curl --negotiate -u : -v -s -I ${url}
    Should contain      ${result}       200 OK

Get SentBytes
    Execute             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'SentBytes' | awk '{ print \$3 }'

Get ReceivedBytes
    Execute             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'ReceivedBytes' | awk '{ print \$3 }'

Get NumActiveClientConnections
    Execute             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'NumActiveClientConnections' | awk '{ print \$3 }'

SentBytes are higher than zero
    ${sentBytes} =              Get SentBytes
                                Should be true      ${sentBytes} > 0

ReceivedBytes are higher than zero
    ${receivedBytes} =          Get ReceivedBytes
                                Should be true      ${receivedBytes} > 0

NumActiveClientConnections are higher than zero
    ${activeConnections} =      Get NumActiveClientConnections
                                Should be true      ${activeConnections} > 0

*** Test Cases ***
Test OM JMX endpoint
    Verify endpoint is up       ${OM_JMX_ENDPOINT}

Check that metrics are registered
    ${result} =         Execute                             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | grep ${GRPC_OM_METRICS_NAME}
                        Should contain      ${result}       ${GRPC_OM_METRICS_NAME}

Check no bytes sent
    ${sentBytes} =              Get SentBytes
                                Should be true      ${sentBytes} == 0

Check no bytes received
    ${receivedBytes} =          Get ReceivedBytes
                                Should be true      ${receivedBytes} == 0

Check no active connections
    ${activeConnections} =      Get NumActiveClientConnections
                                Should be true      ${activeConnections} == 0

Create new bucket
    ${bucket} =         Create bucket

Check bytes sent are higher than zero
#    Run Keyword if      '${GRPC_ENABLED}' == 'true'         SentBytes are higher than zero
    SentBytes are higher than zero

Check bytes received are higher than zero
    Run Keyword if      '${GRPC_ENABLED}' == 'true'         ReceivedBytes are higher than zero

Check active connections are higher than zero
    Run Keyword if      '${GRPC_ENABLED}' == 'true'         NumActiveClientConnections are higher than zero
