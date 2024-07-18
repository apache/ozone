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
Library             Process
Library             BuiltIn
Library             String

*** Variables ***
${URL}                  http://httpfs:14000/webhdfs/v1/

*** Keywords ***
Execute curl command
    [Arguments]       ${path}           ${operation}    ${extra_commands}
    ${user.name} =    Set Variable If   '${SECURITY_ENABLED}'=='false'   &user.name=${USERNAME}      ${EMPTY}
    ${final_url} =    Catenate          SEPARATOR=      ${URL}  ${path}  ?op=  ${operation}     ${user.name}
    ${curl_extra_commands} =            Set Variable If     '${SECURITY_ENABLED}'=='true'       --negotiate -u :    ${EMPTY}
    ${output}         Run process       curl ${extra_commands} ${curl_extra_commands} "${final_url}"    shell=True
    Should Be Equal As Integers         ${output.rc}    0
    [return]          ${output}

Execute create file command
    [Arguments]       ${path}           ${file_name}
    ${user.name} =    Set Variable If   '${SECURITY_ENABLED}'=='false'   &user.name=${USERNAME}      ${EMPTY}
    ${curl_extra_commands} =            Set Variable If     '${SECURITY_ENABLED}'=='true'       --negotiate -u :    ${EMPTY}
    ${final_url} =    Catenate          SEPARATOR=      ${URL}  ${path}  ?op=CREATE     ${user.name}
    ${output}         Run process       curl -X PUT ${curl_extra_commands} "${final_url}"   shell=True
    Should Be Equal As Integers         ${output.rc}    0
    ${final_url2} =   Catenate          SEPARATOR=      ${URL}  ${path}  ?op=CREATE&data=true       ${user.name}
    ${output2}        Run process       curl -X PUT -T ${file_name} ${curl_extra_commands} "${final_url2}" -H"Content-Type: application/octet-stream"   shell=True
    Should Be Equal As Integers         ${output2.rc}    0
    [return]          ${output2}
