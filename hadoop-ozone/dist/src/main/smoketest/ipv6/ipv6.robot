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
Documentation       Checks for a cluster running on an IPv6-only network
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Keywords ***
IPv6 Literal Of
    [Arguments]         ${host}
    ${address} =        Execute             getent hosts ${host} | awk 'NR == 1 { print $1 }'
                        Should Contain      ${address}          :
    [Return]            [${address}]

Address Overrides
    [Arguments]         ${key_prefix}       @{nodes}
    ${options} =        Set Variable        ${EMPTY}
    FOR    ${node}    IN    @{nodes}
        ${literal} =    IPv6 Literal Of     ${node}
        ${options} =    Catenate            ${options}          --set '${key_prefix}.${node}=${literal}'
    END
    [Return]            ${options}

*** Test Cases ***
Hosts have no IPv4 address
    FOR    ${host}    IN    scm1    om1    recon    s3g
        ${rc} =         Run And Return Rc   getent ahostsv4 ${host}
                        Should Not Be Equal As Integers         ${rc}               0
    END

Datanodes register with IPv6 addresses
    ${addresses} =      Execute             ozone admin datanode list --json | jq -r '.[].ipAddress'
                        Should Contain      ${addresses}        :
                        Should Not Match Regexp                 ${addresses}        \\d+\\.\\d+\\.\\d+\\.\\d+

Datanodes are reachable by IPv6 address
    ${output} =         Execute             ozone admin reconfig --service=DATANODE --in-service-datanodes properties
                        Should Contain X Times                  ${output}           Reconfigurable properties    3

HTTP servers listen on IPv6
    FOR    ${host}    ${port}    IN    scm1    9876    om1    9874    recon    9888    s3g    19878
        ${literal} =    IPv6 Literal Of     ${host}
                        Execute             curl -g -sSf -o /dev/null 'http://${literal}:${port}/jmx'
    END

SCM is reachable by IPv6 literal
    ${overrides} =      Address Overrides   ozone.scm.address.scmservice            scm1    scm2    scm3
    ${output} =         Execute             ozone admin ${overrides} scm roles
                        Should Contain      ${output}           LEADER

OM is reachable by IPv6 literal
    ${overrides} =      Address Overrides   ozone.om.address.omservice              om1     om2     om3
                        Execute             ozone sh ${overrides} volume list o3://omservice/
