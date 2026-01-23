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
Documentation       Test om compatibility
Resource            lib.resource
Test Timeout        5 minutes

*** Test Cases ***
Picks up command line options
    Pass Execution If    '%{HDFS_OM_OPTS}' == ''    Command-line option required for process check
    ${processes} =    Wait for server command-line options
    Should Contain    ${processes}   %{HDFS_OM_OPTS}
    Check client command-line options

Rejects Atomic Key Rewrite
    Execute           ozone freon ockg -n1 -t1 -p rewrite
    ${output} =       Execute and check rc    ozone sh key rewrite -t EC -r rs-3-2-1024k /vol1/bucket1/rewrite/0    255
    Should Contain    ${output}    Feature disabled: ATOMIC_REWRITE_KEY
