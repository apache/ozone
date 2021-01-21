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
Resource    ../lib/os.robot
Library     String


*** Keywords ***
Bucket Exists
    [arguments]    ${bucket}
    ${rc}    ${output} =      Run And Return Rc And Output             timeout 15 ozone sh bucket info ${bucket}
    Return From Keyword If    ${rc} != 0                               ${FALSE}
    Return From Keyword If    'VOLUME_NOT_FOUND' in '''${output}'''    ${FALSE}
    Return From Keyword If    'BUCKET_NOT_FOUND' in '''${output}'''    ${FALSE}
    [Return]                  ${TRUE}

Compare Key With Local File
    [arguments]    ${key}    ${file}
    ${postfix} =   Generate Random String  5  [NUMBERS]
    ${tmpfile} =   Set Variable    /tmp/tempkey-${postfix}
    Execute        ozone sh key get -f ${key} ${tmpfile}
    ${rc} =        Run And Return Rc    diff -q ${file} ${tmpfile}
    Execute        rm -f ${tmpfile}
    ${result} =    Set Variable If    ${rc} == 0    ${TRUE}   ${FALSE}
    [Return]       ${result}

Key Should Match Local File
    [arguments]    ${key}    ${file}
    ${matches} =   Compare Key With Local File    ${key}    ${file}
    Should Be True    ${matches}

Verify ACL
    [arguments]         ${object_type}   ${object}    ${type}   ${name}    ${acls}
    ${actual_acls} =    Execute          ozone sh ${object_type} getacl ${object} | jq -r '.[] | select(.type == "${type}") | select(.name == "${name}") | .aclList[]' | xargs
                        Should Be Equal    ${acls}    ${actual_acls}

Create Random Volume
    ${random} =    Generate Random String  5  [LOWER]
    Execute        ozone sh volume create o3://${OM_SERVICE_ID}/vol-${random}
    [return]       vol-${random}

Find Jars Dir
    ${dir} =    Execute    ozone envvars | grep 'HDDS_LIB_JARS_DIR' | cut -f2 -d= | sed -e "s/'//g" -e 's/"//g'
    Set Environment Variable    HDDS_LIB_JARS_DIR    ${dir}
