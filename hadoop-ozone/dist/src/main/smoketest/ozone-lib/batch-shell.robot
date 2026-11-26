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
Documentation    Ozone Shell operations using Batch
Resource         ../ozone-lib/batch.robot


*** Keywords ***
Put and Get Key Later
    [arguments]    ${put_to}    ${get_from}=${put_to}    ${put_from}=/etc/passwd
    Execute Later ozone sh    key put ${put_to} ${put_from}
    ${get_to} =    Get Key Later    ${get_from}
    Assert Later    Compare Files    ${put_from}    ${get_to}


Get Key Later
    [arguments]    ${get_from}
    ${get_to} =    Get Next Filename    ${BATCH_OUTPUT_DIR}/key-get
    Execute Later ozone sh    key get -f ${get_from} ${get_to}
    [return]    ${get_to}

Assert Later Volume Does Not Exist
    [arguments]    ${volume}
    ${result} =     Execute Later ozone sh    volume info ${volume}
    Assert Later    File ${result} Should    Contain    VOLUME_NOT_FOUND
