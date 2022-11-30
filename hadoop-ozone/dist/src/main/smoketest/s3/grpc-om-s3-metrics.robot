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
Suite Setup         Setup v4 headers

*** Variables ***
${GRPC_ENABLED}             false
${OM_URL}                   http://${OM_SERVICE_ID}:9874
${OM_JMX_ENDPOINT}          ${OM_URL}/jmx
${GRPC_OM_METRICS_NAME}     GrpcOzoneManagerMetrics

*** Keywords ***
Verify endpoint is up
    [arguments]         ${url}
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
    ${result} =         Execute                             curl --negotiate -u : -v -s -I ${url}
    Should contain      ${result}       200 OK

Get SentBytes
    ${sentBytes} =              Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'SentBytes' | grep -Eo '[0-9]{1,}'
    [return]                    ${sentBytes}

Get ReceivedBytes
    ${receivedBytes} =          Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'ReceivedBytes' | grep -Eo '[0-9]{1,}'
    [return]                    ${receivedBytes}

Get NumActiveClientConnections
    ${activeConnections} =      Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_OM_METRICS_NAME}/,/}/p' | grep 'NumActiveClientConnections' | grep -Eo '[0-9]{1,}'
    [return]                    ${activeConnections}

SentBytes are equal to zero
    ${sentBytes} =                  Get SentBytes
                                    Should be true      ${sentBytes} == 0

ReceivedBytes are equal to zero
    ${receivedBytes} =              Get ReceivedBytes
                                    Should be true      ${receivedBytes} == 0

NumActiveClientConnections are equal to zero
    ${activeConnections} =          Get NumActiveClientConnections
                                    Should be true      ${activeConnections} == 0

SentBytes are higher than zero
    ${sentBytes} =                      Get SentBytes
                                        Should be true      ${sentBytes} > 0

ReceivedBytes are higher than zero
    ${receivedBytes} =                  Get ReceivedBytes
                                        Should be true      ${receivedBytes} > 0

NumActiveClientConnections are higher than zero
    ${activeConnections} =              Get NumActiveClientConnections
                                        Should be true      ${activeConnections} > 0

*** Test Cases ***
Test OM JMX endpoint
    Verify endpoint is up       ${OM_JMX_ENDPOINT}

Check that metrics are registered
    ${result} =         Execute                             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | grep ${GRPC_OM_METRICS_NAME}
                        Should contain      ${result}       ${GRPC_OM_METRICS_NAME}

Check no bytes sent
    SentBytes are equal to zero

Check no bytes received
    ReceivedBytes are equal to zero

Check no active connections
    NumActiveClientConnections are equal to zero

Create new bucket
    ${bucket} =         Create bucket

Check bytes sent are higher than zero
    IF      '${GRPC_ENABLED}' == 'true'
        SentBytes are higher than zero
    ELSE
        SentBytes are equal to zero
    END

Check bytes received are higher than zero
    IF      '${GRPC_ENABLED}' == 'true'
        ReceivedBytes are higher than zero
    ELSE
        ReceivedBytes are equal to zero
    END

Check active connections are higher than zero
    IF      '${GRPC_ENABLED}' == 'true'
        NumActiveClientConnections are higher than zero
    ELSE
        NumActiveClientConnections are equal to zero
    END
