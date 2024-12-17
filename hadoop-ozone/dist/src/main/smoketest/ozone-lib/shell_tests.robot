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
Resource    shell.robot


*** Variables ***
${OM_SERVICE_ID}     om


*** Test Cases ***

Bucket Exists should not if No Such Volume
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/no-such-volume/any-bucket
    Should Be Equal             ${exists}       ${FALSE}

Bucket Exists should not if No Such Bucket
    Execute And Ignore Error    ozone sh volume create o3://${OM_SERVICE_ID}/vol1
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/vol1/no-such-bucket
    Should Be Equal             ${exists}       ${FALSE}

Bucket Exists
    Execute And Ignore Error    ozone sh bucket create o3://${OM_SERVICE_ID}/vol1/bucket
    ${exists} =                 Bucket Exists   o3://${OM_SERVICE_ID}/vol1/bucket
    Should Be Equal             ${exists}       ${TRUE}

Bucket Exists should not if No Such OM service
    ${exists} =                 Bucket Exists   o3://no-such-host/any-volume/any-bucket
    Should Be Equal             ${exists}       ${FALSE}


Key Should Match Local File
    [Setup]                     Execute    ozone sh key put o3://${OM_SERVICE_ID}/vol1/bucket/passwd /etc/passwd
    Key Should Match Local File     o3://${OM_SERVICE_ID}/vol1/bucket/passwd    /etc/passwd

Compare Key With Local File with Different File
    ${random_file} =            Create Random File KB    42
    ${matches} =                Compare Key With Local File     o3://${OM_SERVICE_ID}/vol1/bucket/passwd    ${random_file}
    Should Be Equal             ${matches}     ${FALSE}
    [Teardown]                  Remove File    ${random_file}

Compare Key With Local File if File Does Not Exist
    ${matches} =                Compare Key With Local File     o3://${OM_SERVICE_ID}/vol1/bucket/passwd    /no-such-file
    Should Be Equal             ${matches}     ${FALSE}

Rejects Put Key With Zero Expected Generation
    ${output} =     Execute and checkrc    ozone sh key put --expectedGeneration 0 o3://${OM_SERVICE_ID}/vol1/bucket/passwd /etc/passwd    255
    Should Contain    ${output}    must be positive

Rejects Put Key With Negative Expected Generation
    ${output} =     Execute and checkrc    ozone sh key put --expectedGeneration -1 o3://${OM_SERVICE_ID}/vol1/bucket/passwd /etc/passwd    255
    Should Contain    ${output}    must be positive
