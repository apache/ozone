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
Documentation       Test EC shell commands
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot
Resource            ../s3/commonawslib.robot
Resource            lib.resource
Suite Setup         Kinit test user     testuser     testuser.keytab


*** Variables ***
${ENDPOINT_URL}       http://s3g:9878


*** Test Cases ***

Rewrite Multipart Key
    [setup]    Setup v4 headers
    ${bucket} =    Create bucket with layout    /s3v    OBJECT_STORE
    ${key} =    Set Variable    multipart.key
    ${file} =    Create Random File MB    12
    Execute AWSS3Cli    cp ${file} s3://${bucket}/${key}
    Key Should Match Local File    /s3v/${bucket}/${key}    ${file}
    Verify Key Replica Replication Config    /s3v/${bucket}/${key}    RATIS    THREE

    Execute    ozone sh key rewrite -t EC -r rs-3-2-1024k /s3v/${bucket}/${key}

    Key Should Match Local File    /s3v/${bucket}/${key}    ${file}
    Verify Key EC Replication Config    /s3v/${bucket}/${key}    RS    3    2    1048576
