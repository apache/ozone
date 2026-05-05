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
Library             BuiltIn
Library             OperatingSystem
Resource            ../lib/os.robot

*** Keywords ***
Check server command-line options
    ${processes} =    List All Processes
    Should Contain    ${processes}   %{HADOOP_OPTS}
    Should Contain    ${processes}   %{OZONE_SERVER_OPTS}
    RETURN    ${processes}

Wait for server command-line options
    ${processes} =     Wait Until Keyword Succeeds    3min    2sec    Check server command-line options
    RETURN    ${processes}

Check client command-line options
    Set Environment Variable    OZONE_SHELL_SCRIPT_DEBUG    true
    ${output} =    Execute    ozone sh --help
    Should Not Contain    ${output}    %{OZONE_SERVER_OPTS}
