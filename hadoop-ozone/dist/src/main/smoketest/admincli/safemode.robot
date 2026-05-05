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
Documentation       Test ozone admin safemode command
Library             BuiltIn
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${SCM}       scm

*** Test Cases ***
Check safemode
    ${output} =         Execute          ozone admin safemode status
                        Should contain   ${output}   SCM is out of safe mode

Check safemode with explicit host
    ${output} =         Execute          ozone admin safemode status --scm ${SCM}
                        Should contain   ${output}   SCM is out of safe mode

Wait for safemode exit
    ${output} =         Execute          ozone admin safemode wait -t 2
                        Should contain   ${output}   SCM is out of safe mode

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin safemode
                        Should contain   ${output}   Missing required subcommand
                        Should contain   ${output}   status
                        Should contain   ${output}   exit
                        Should contain   ${output}   wait

#Check safemode on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose safemode status --scm unknown-host
#                        Should contain   ${output}   Invalid host name

