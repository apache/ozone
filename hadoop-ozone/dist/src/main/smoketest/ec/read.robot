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
Documentation       Test reading EC keys
Resource            ../ozone-lib/shell.robot

*** Variables ***
${PREFIX}    ${EMPTY}
${VOLUME}    vol${PREFIX}

*** Test Cases ***
Read 1MB EC Key
    Key Should Match Local File    /${VOLUME}/ecbucket/dir/1mb      /tmp/1mb

Read 2MB EC Key
    Key Should Match Local File    /${VOLUME}/ecbucket/dir/2mb      /tmp/2mb

Read 3MB EC Key
    Key Should Match Local File    /${VOLUME}/ecbucket/dir/3mb      /tmp/3mb

Read 100MB EC Key
    Key Should Match Local File    /${VOLUME}/ecbucket/dir/100mb    /tmp/100mb

Read EC Key in Ratis Bucket
    Key Should Match Local File    /${VOLUME}/ratis/dir2/1mbEC    /tmp/1mb
