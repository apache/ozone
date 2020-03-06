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
Documentation       Smoke test to start cluster with docker-compose environments.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../commonlib.robot

*** Variables ***
${ENDPOINT_URL}       http://recon:9888
${API_ENDPOINT_URL}   http://recon:9888/api/v1

*** Test Cases ***
Recon OM APIs
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab
                        Execute                             ozone freon rk --numOfVolumes 1 --numOfBuckets 1 --numOfKeys 10 --keySize 1025
                        Sleep               90s

    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -v ${API_ENDPOINT_URL}/containers
                        Should contain      ${result}       containers
                        Should contain      ${result}       \"ContainerID\":1

    ${result} =         Execute                             curl --negotiate -u : -v ${API_ENDPOINT_URL}/utilization/fileCount
                        Should contain      ${result}       \"fileSize\":2048,\"count\":10

Recon SCM APIs
    ${result} =         Execute                             curl --negotiate -u : -v ${API_ENDPOINT_URL}/datanodes
                        Should contain      ${result}       datanodes
                        Should contain      ${result}       datanode_1
                        Should contain      ${result}       datanode_2
                        Should contain      ${result}       datanode_3

    ${result} =         Execute                             curl --negotiate -u : -v ${API_ENDPOINT_URL}/pipelines
                        Should contain      ${result}       pipelines
                        Should contain      ${result}       RATIS
                        Should contain      ${result}       OPEN
                        Should contain      ${result}       datanode_1
                        Should contain      ${result}       datanode_2
                        Should contain      ${result}       datanode_3
Recon Web UI
    ${result} =         Execute                             curl --negotiate -u : -v ${ENDPOINT_URL}
                        Should contain      ${result}       Ozone Recon