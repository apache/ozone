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
Documentation       Read Compatibility
Resource            ../ozone-lib/shell.robot
Test Timeout        5 minutes

*** Variables ***
${SUFFIX}    ${EMPTY}

*** Test Cases ***
Key Can Be Read
    Key Should Match Local File    /vol1/bucket1/key-${SUFFIX}    /etc/passwd

Dir Can Be Listed
    Execute    ozone fs -ls o3fs://bucket1.vol1/dir-${SUFFIX}

File Can Be Get
    Execute    ozone fs -get o3fs://bucket1.vol1/dir-${SUFFIX}/passwd /tmp/passwd-${SUFFIX}
    [teardown]    Execute    rm /tmp/passwd-${SUFFIX}
