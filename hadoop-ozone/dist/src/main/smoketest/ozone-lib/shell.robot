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
Ozone Shell Batch
    [arguments]    @{commands}
    ${cmd} =    Catenate    SEPARATOR=' --execute '    @{commands}
    Run Keyword And Return    Execute and checkrc    ozone sh --execute '${cmd}'    0

Bucket Exists
    [arguments]    ${bucket}
    ${rc}    ${output} =      Run And Return Rc And Output             timeout 15 ozone sh bucket info ${bucket}
    Return From Keyword If    ${rc} != 0                               ${FALSE}
    Return From Keyword If    'VOLUME_NOT_FOUND' in '''${output}'''    ${FALSE}
    Return From Keyword If    'BUCKET_NOT_FOUND' in '''${output}'''    ${FALSE}
    [Return]                  ${TRUE}

Compare Key With Local File
    [arguments]    ${key}    ${file}    ${cmd}=sh key get
    ${postfix} =   Generate Random String  5  [NUMBERS]
    ${tmpfile} =   Set Variable    /tmp/tempkey-${postfix}
    Execute        ozone ${cmd} ${key} ${tmpfile}
    ${rc} =        Run And Return Rc    diff -q ${file} ${tmpfile}
    Execute        rm -f ${tmpfile}
    ${result} =    Set Variable If    ${rc} == 0    ${TRUE}   ${FALSE}
    [Return]       ${result}

Key Should Match Local File
    [arguments]    ${key}    ${file}
    ${matches} =   Compare Key With Local File    ${key}    ${file}
    Should Be True    ${matches}

File Should Match Local File
    [arguments]    ${key}    ${file}
    ${matches} =   Compare Key With Local File    ${key}    ${file}    fs -get
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
    [arguments]    ${ozone_dir}=${OZONE_DIR}
    ${dir} =    Execute    ${ozone_dir}/bin/ozone envvars | grep 'HDDS_LIB_JARS_DIR' | cut -f2 -d= | sed -e "s/'//g" -e 's/"//g'
    [return]    ${dir}

Create bucket with layout
    [Arguments]          ${volume}    ${layout}
    ${postfix} =         Generate Random String    10    [LOWER]
    ${bucket} =          Set Variable    bucket-${postfix}
    ${result} =          Execute         ozone sh bucket create --layout ${layout} ${volume}/${bucket}
    [Return]             ${bucket}

Create Key
    [arguments]    ${key}    ${file}    ${args}=${EMPTY}
    ${output} =    Execute          ozone sh key put ${args} ${key} ${file}
                   Should not contain  ${output}       Failed
    Log            Uploaded ${file} to ${key}

Assert Unsupported
    [arguments]    ${cmd}
    ${result} =     Execute and checkrc         ${cmd}       255
                    Should Contain  ${result}   NOT_SUPPORTED_OPERATION

Verify Bucket Empty Replication Config
    [arguments]    ${bucket}
    ${result} =    Execute                      ozone sh bucket info ${bucket} | jq -r '.replicationConfig'
                   Should Be Equal          ${result}       null

Verify Bucket Replica Replication Config
    [arguments]    ${bucket}    ${type}    ${factor}
    ${result} =    Execute                      ozone sh bucket info ${bucket} | jq -r '.replicationConfig.replicationType, .replicationConfig.replicationFactor'
                   Verify Replica Replication Config    ${result}   ${type}     ${factor}

Verify Key Replica Replication Config
    [arguments]    ${key}    ${type}    ${factor}
    ${result} =    Execute                      ozone sh key info ${key} | jq -r '.replicationConfig.replicationType, .replicationConfig.replicationFactor'
                   Verify Replica Replication Config    ${result}   ${type}     ${factor}

Verify Replica Replication Config
    [arguments]    ${result}    ${type}    ${factor}
                   Should Match Regexp      ${result}       ^(?m)${type}$
                   Should Match Regexp      ${result}       ^(?m)${factor}$

Verify Bucket EC Replication Config
    [arguments]    ${bucket}    ${encoding}    ${data}    ${parity}    ${chunksize}
    ${result} =    Execute                      ozone sh bucket info ${bucket} | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                   Verify EC Replication Config     ${result}    ${encoding}    ${data}    ${parity}    ${chunksize}

Verify Key EC Replication Config
    [arguments]    ${key}    ${encoding}    ${data}    ${parity}    ${chunksize}
    ${result} =    Execute                      ozone sh key info ${key} | jq -r '.replicationConfig.replicationType, .replicationConfig.codec, .replicationConfig.data, .replicationConfig.parity, .replicationConfig.ecChunkSize'
                   Verify EC Replication Config     ${result}    ${encoding}    ${data}    ${parity}    ${chunksize}

Verify EC Replication Config
    [arguments]    ${result}    ${encoding}    ${data}    ${parity}    ${chunksize}
                   Should Match Regexp      ${result}       ^(?m)EC$
                   Should Match Regexp      ${result}       ^(?m)${encoding}$
                   Should Match Regexp      ${result}       ^(?m)${data}$
                   Should Match Regexp      ${result}       ^(?m)${parity}$
                   Should Match Regexp      ${result}       ^(?m)${chunksize}$
