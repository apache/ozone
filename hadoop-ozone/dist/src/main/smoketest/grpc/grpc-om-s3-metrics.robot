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
Resource            ../s3/commonawslib.robot
Test Timeout        5 minutes
Suite Setup         Setup s3 tests

*** Variables ***
${OM_URL}                   http://${OM_SERVICE_ID}:9874
${OM_JMX_ENDPOINT}          ${OM_URL}/jmx
${GRPC_METRICS_NAME}        GrpcMetrics

*** Keywords ***
Check gRPC conf
    ${confKey} =        Execute And Ignore Error        ozone getconf confKey ozone.om.transport.class
    ${result} =         Evaluate                        "GrpcOmTransport" in """${confKey}"""
    IF      ${result} == ${True}
        Set Suite Variable      ${GRPC_ENABLED}         true
    ELSE
        Set Suite Variable      ${GRPC_ENABLED}         false
    END

Verify endpoint is up
    [arguments]         ${url}
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -v -s -I ${url}
    Should contain      ${result}       200 OK

Get SentBytes
    ${sentBytes} =              Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_METRICS_NAME}/,/}/p' | grep 'SentBytes' | grep -Eo '[0-9]{1,}'
    [return]                    ${sentBytes}

Get ReceivedBytes
    ${receivedBytes} =          Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_METRICS_NAME}/,/}/p' | grep 'ReceivedBytes' | grep -Eo '[0-9]{1,}'
    [return]                    ${receivedBytes}

Get NumOpenClientConnections
    ${activeConnections} =      Execute         curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | sed -n '/${GRPC_METRICS_NAME}/,/}/p' | grep 'NumOpenClientConnections' | grep -Eo '[0-9]{1,}'
    [return]                    ${activeConnections}

SentBytes are equal to zero
    ${sentBytes} =                  Get SentBytes
                                    Should be true      ${sentBytes} == 0

ReceivedBytes are equal to zero
    ${receivedBytes} =              Get ReceivedBytes
                                    Should be true      ${receivedBytes} == 0

NumOpenClientConnections are equal to zero
    ${activeConnections} =          Get NumOpenClientConnections
                                    Should be true      ${activeConnections} == 0

SentBytes are higher than zero
    ${sentBytes} =                      Get SentBytes
                                        Should be true      ${sentBytes} > 0

ReceivedBytes are higher than zero
    ${receivedBytes} =                  Get ReceivedBytes
                                        Should be true      ${receivedBytes} > 0

NumOpenClientConnections are higher than zero
    ${activeConnections} =              Get NumOpenClientConnections
                                        Should be true      ${activeConnections} > 0

*** Test Cases ***
Test gRPC conf
    Check gRPC conf

Test OM JMX endpoint
    Verify endpoint is up       ${OM_JMX_ENDPOINT}

Check that metrics are registered
    ${result} =         Execute                             curl --negotiate -u : -LSs ${OM_JMX_ENDPOINT} | grep ${GRPC_METRICS_NAME}
                        Should contain      ${result}       ${GRPC_METRICS_NAME}

Check bytes sent
    IF      '${GRPC_ENABLED}' == 'true'
        Wait Until Keyword Succeeds     90sec      10sec        SentBytes are higher than zero
    ELSE
        SentBytes are equal to zero
    END

Check bytes received
    IF      '${GRPC_ENABLED}' == 'true'
        Wait Until Keyword Succeeds     90sec      10sec        ReceivedBytes are higher than zero
    ELSE
        ReceivedBytes are equal to zero
    END

Check active connections
    IF      '${GRPC_ENABLED}' == 'true'
        Wait Until Keyword Succeeds     90sec      10sec        NumOpenClientConnections are higher than zero
    ELSE
        NumOpenClientConnections are equal to zero
    END
