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
Documentation       Test HSync via freon CLI.
Library             OperatingSystem
Library             String
Library             BuiltIn
Resource            ../ozone-lib/freon.robot
Resource            ../lib/fs.robot
Test Timeout        10 minutes
Suite Setup         Create volume and bucket

*** Variables ***
${OM_SERVICE_ID}    %{OM_SERVICE_ID}
${VOLUME}           hsync-volume
${BUCKET}           hsync-bucket

*** Keywords ***
Create volume and bucket
    Execute             ozone sh volume create /${volume}
    Execute             ozone sh bucket create /${volume}/${bucket}

*** Test Cases ***
Generate key for o3fs by HSYNC
    ${path} =     Format FS URL         o3fs     ${VOLUME}    ${BUCKET}
    Freon DFSG    sync=HSYNC    path=${path}

Generate key for o3fs by HFLUSH
    ${path} =     Format FS URL         o3fs     ${VOLUME}    ${BUCKET}
    Freon DFSG    sync=HFLUSH   path=${path}

Generate key for ofs by HSYNC
    ${path} =     Format FS URL         ofs     ${VOLUME}    ${BUCKET}
    Freon DFSG    sync=HSYNC    path=${path}

Generate key for ofs by HFLUSH
    ${path} =     Format FS URL         ofs     ${VOLUME}    ${BUCKET}
    Freon DFSG    sync=HFLUSH   path=${path}
