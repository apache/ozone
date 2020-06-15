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
Library             OperatingSystem
Library             String
Library             BuiltIn

*** Variables ***
${SECURITY_ENABLED}                 %{SECURITY_ENABLED}
${OM_HA_PARAM}                      %{OM_HA_PARAM}
${OM_SERVICE_ID}                    %{OM_SERVICE_ID}

*** Keywords ***
Execute
    [arguments]                     ${command}
    ${rc}                           ${output} =                 Run And Return Rc And Output           ${command}
    Log                             ${output}
    Should Be Equal As Integers     ${rc}                       0
    [return]                        ${output}

Execute And Ignore Error
    [arguments]                     ${command}
    ${rc}                           ${output} =                 Run And Return Rc And Output           ${command}
    Log                             ${output}
    [return]                        ${output}

Execute and checkrc
    [arguments]                     ${command}                  ${expected_error_code}
    ${rc}                           ${output} =                 Run And Return Rc And Output           ${command}
    Log                             ${output}
    Should Be Equal As Integers     ${rc}                       ${expected_error_code}
    [return]                        ${output}

Compare files
    [arguments]                 ${file1}                   ${file2}
    ${checksumbefore} =         Execute                    md5sum ${file1} | awk '{print $1}'
    ${checksumafter} =          Execute                    md5sum ${file2} | awk '{print $1}'
                                Should Be Equal            ${checksumbefore}            ${checksumafter}

Install aws cli
    ${rc}              ${output} =                 Run And Return Rc And Output           which apt-get
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 debian
    ${rc}              ${output} =                 Run And Return Rc And Output           yum --help
    Run Keyword if     '${rc}' == '0'              Install aws cli s3 centos

Kinit HTTP user
    ${hostname} =       Execute                    hostname
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k HTTP/${hostname}@EXAMPLE.COM -t /etc/security/keytabs/HTTP.keytab

Kinit test user
    [arguments]                      ${user}       ${keytab}
    ${hostname} =       Execute                    hostname
    Set Suite Variable  ${TEST_USER}               ${user}/${hostname}@EXAMPLE.COM
    Wait Until Keyword Succeeds      2min       10sec      Execute            kinit -k ${user}/${hostname}@EXAMPLE.COM -t /etc/security/keytabs/${keytab}

Should Match Local File
    [arguments]    ${key}    ${file}
    Execute        ozone sh key get -f ${key} /tmp/tempkey
    Execute        diff -q ${file} /tmp/tempkey
    Execute        rm -f /tmp/tempkey

Verify ACL
    [arguments]         ${object_type}   ${object}    ${type}   ${name}    ${acls}
    ${actual_acls} =    Execute          ozone sh ${object_type} getacl ${object} | jq -r '.[] | select(.type == "${type}") | select(.name == "${name}") | .aclList[]' | xargs
                        Should Be Equal    ${acls}    ${actual_acls}

Create volume via shell
    [Arguments]         ${volume}
    ${result} =         Execute And Ignore Error      ozone sh volume create o3://${OM_SERVICE_ID}/${volume}
                        Should not contain            ${result}          Failed

Create bucket via shell
    [Arguments]         ${volume}    ${bucket}
    ${result} =         Execute And Ignore Error      ozone sh bucket create o3://${OM_SERVICE_ID}/${volume}/${bucket}
                        Should not contain            ${result}          Failed

Link bucket via shell
    [Arguments]         ${source_volume}    ${source_bucket}    ${target_volume}    ${target_bucket}
    ${result} =         Execute And Ignore Error      ozone sh bucket link o3://${OM_SERVICE_ID}/${source_volume}/${source_bucket} o3://${OM_SERVICE_ID}/${target_volume}/${target_bucket}
                        Should not contain            ${result}          Failed

Generate random name
    [Arguments]         ${prefix}
    ${postfix} =        Generate Random String  5            [NUMBERS]
    [Return]            ${prefix}${postfix}

Put key via shell
    [Arguments]         ${volume}    ${bucket}    ${key}    ${file}
    Execute             ozone sh key put o3://${OM_SERVICE_ID}/${volume}/${bucket}/${key} ${file}

Create test key
    [Arguments]          ${volume}    ${bucket}    ${prefix}    ${file}
    ${key} =             Generate random name      ${prefix}
    Put key via shell    ${volume}    ${bucket}    ${key}    ${file}
    [Return]             ${key}
