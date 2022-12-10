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
Resource            ../ozone-lib/freon.robot
Test Timeout        5 minutes

*** Variables ***
${ENDPOINT_URL}       http://recon:9888
${API_ENDPOINT_URL}   ${ENDPOINT_URL}/api/v1
${ADMIN_API_ENDPOINT_URL}   ${API_ENDPOINT_URL}/containers
${UNHEALTHY_ENDPOINT_URL}   ${API_ENDPOINT_URL}/containers/unhealthy
${NON_ADMIN_API_ENDPOINT_URL}   ${API_ENDPOINT_URL}/clusterState

*** Keywords ***
Check if Recon picks up container from OM
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit as ozone admin
    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/containers
                        Should contain      ${result}       \"ContainerID\"

    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/utilization/fileCount
                        Should contain      ${result}       \"fileSize\":2048,\"count\":10

Kinit as non admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     scm     scm.keytab

Kinit as ozone admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser     testuser.keytab

Kinit as recon admin
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit test user     testuser2           testuser2.keytab

Check http return code
    [Arguments]         ${url}          ${expected_code}
    ${result} =         Execute                             curl --negotiate -u : --write-out '\%{http_code}\n' --silent --show-error --output /dev/null ${url}
                        IF  '${SECURITY_ENABLED}' == 'true'
                            Should contain      ${result}       ${expected_code}
                        ELSE
                            # All access should succeed without security.
                            Should contain      ${result}       200
                        END

*** Test Cases ***
Check if Recon picks up OM data
    [Setup]    Freon OCKG    n=10    args=-s 1025 -v recon -b api
    Wait Until Keyword Succeeds     90sec      10sec        Check if Recon picks up container from OM

Check if Recon picks up DN heartbeats
    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/datanodes
                        Should contain      ${result}       datanodes
                        Should contain      ${result}       datanode_1
                        Should contain      ${result}       datanode_2
                        Should contain      ${result}       datanode_3

    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/pipelines
                        Should contain      ${result}       pipelines
                        Should contain      ${result}       RATIS
                        Should contain      ${result}       OPEN
                        Should contain      ${result}       datanode_1
                        Should contain      ${result}       datanode_2
                        Should contain      ${result}       datanode_3

    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/clusterState
                        Should contain      ${result}       \"totalDatanodes\"
                        Should contain      ${result}       \"healthyDatanodes\"
                        Should contain      ${result}       \"pipelines\"

    ${result} =         Execute                             curl --negotiate -u : -LSs ${API_ENDPOINT_URL}/containers/1/replicaHistory
                        Should contain      ${result}       \"containerId\":1

Check if Recon Web UI is up
    Run Keyword if      '${SECURITY_ENABLED}' == 'true'     Kinit HTTP user
    ${result} =         Execute                             curl --negotiate -u : -LSs ${ENDPOINT_URL}
                        Should contain      ${result}       Ozone Recon

Check web UI access
    # Unauthenticated user cannot access web UI, but any authenticated user can.
    Execute    kdestroy
    Check http return code      ${ENDPOINT_URL}     401

    kinit as non admin
    Check http return code      ${ENDPOINT_URL}     200

Check admin only api access
    Execute    kdestroy
    Check http return code      ${ADMIN_API_ENDPOINT_URL}       401

    kinit as non admin
    Check http return code      ${ADMIN_API_ENDPOINT_URL}       403

    kinit as ozone admin
    Check http return code      ${ADMIN_API_ENDPOINT_URL}       200

    kinit as recon admin
    Check http return code      ${ADMIN_API_ENDPOINT_URL}       200

Check unhealthy, (admin) api access
    Execute    kdestroy
    Check http return code      ${UNHEALTHY_ENDPOINT_URL}       401

    kinit as non admin
    Check http return code      ${UNHEALTHY_ENDPOINT_URL}       403

    kinit as ozone admin
    Check http return code      ${UNHEALTHY_ENDPOINT_URL}       200

    kinit as recon admin
    Check http return code      ${UNHEALTHY_ENDPOINT_URL}       200

Check normal api access
    Execute    kdestroy
    Check http return code      ${NON_ADMIN_API_ENDPOINT_URL}   401

    kinit as non admin
    Check http return code      ${NON_ADMIN_API_ENDPOINT_URL}   200
