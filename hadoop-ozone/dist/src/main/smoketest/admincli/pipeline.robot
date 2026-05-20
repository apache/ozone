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
Documentation       Test ozone admin pipeline command
Library             BuiltIn
Resource            ../commonlib.robot
Suite Setup         Kinit test user     testuser     testuser.keytab
Test Timeout        5 minutes

*** Variables ***
${PIPELINE}
${SCM}       scm

*** Test Cases ***
List pipelines
    ${output} =         Execute          ozone admin pipeline list
                        Should contain   ${output}   RATIS/ONE
    ${pipeline} =       Execute          ozone admin pipeline list | grep 'ReplicationConfig: RATIS/ONE' | head -n 1 | cut -d' ' -f3 | sed 's/,$//'
                        Set Suite Variable    ${PIPELINE}    ${pipeline}

List pipeline with json option
    ${output} =         Execute          ozone admin pipeline list --json | jq 'map(.replicationConfig) | contains([{"replicationFactor": "ONE", "replicationType": "RATIS"}])'
    Should be true      $output

List pipelines with explicit host
    ${output} =         Execute          ozone admin pipeline list --scm ${SCM}
                        Should contain   ${output}   RATIS/ONE

List pipelines with explicit host and json option
    ${output} =         Execute   ozone admin pipeline list --scm ${SCM} --json | jq 'map(.replicationConfig) | contains([{"replicationFactor": "ONE", "replicationType": "RATIS"}])'

Deactivate pipeline
                        Execute          ozone admin pipeline deactivate "${PIPELINE}"
    ${output} =         Execute          ozone admin pipeline list | grep "${PIPELINE}"
                        Should contain   ${output}   DORMANT

Activate pipeline
                        Execute          ozone admin pipeline activate "${PIPELINE}"
    ${output} =         Execute          ozone admin pipeline list | grep "${PIPELINE}"
                        Should contain   ${output}   OPEN

Close pipeline
                        Execute          ozone admin pipeline close "${PIPELINE}"
    ${output} =         Execute          ozone admin pipeline list
                        Pass Execution If     '${PIPELINE}' not in '''${output}'''    Pipeline already scrubbed
                        Should Match Regexp   ${output}   ${PIPELINE}.*CLOSED

Incomplete command
    ${output} =         Execute And Ignore Error     ozone admin pipeline
                        Should contain   ${output}   Missing required subcommand
                        Should contain   ${output}   close
                        Should contain   ${output}   create
                        Should contain   ${output}   deactivate
                        Should contain   ${output}   list

Create pipeline
    ${output} =         Execute And Ignore Error     ozone admin pipeline create -t RATIS -f ONE
                        Should Contain Any   ${output}   is created.   RATIS/ONE   Cannot create pipeline

#List pipelines on unknown host
#    ${output} =         Execute And Ignore Error     ozone admin --verbose pipeline list --scm unknown-host
#                        Should contain   ${output}   Invalid host name
