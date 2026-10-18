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
Documentation       Test short-circuit read feature
Library             OperatingSystem
Library             String
Resource            ../commonlib.robot
Test Timeout        5 minutes

*** Variables ***
${VOLUME}           sc-vol
${BUCKET}           sc-bucket
${KEY}              sc-key

*** Test Cases ***
Test Short Circuit Read Metrics
    Pass Execution If   '${SHORT_CIRCUIT_READ_ENABLED}' == 'false'    Skip when short-circuit read is disabled

    ${random} =         Generate Random String  5  [NUMBERS]
    ${vol} =            Set Variable  ${VOLUME}${random}
    ${buck} =           Set Variable  ${BUCKET}${random}

    # Create volume and bucket
    Execute             ozone sh volume create /${vol}
    Execute             ozone sh bucket create /${vol}/${buck}

    # Create a dummy file
    Execute             dd if=/dev/urandom of=/tmp/testfile bs=1024 count=1024

    # Put key
    Execute             ozone sh key put /${vol}/${buck}/${KEY} /tmp/testfile

    # Get key
    ${result} =         Execute             ozone sh key get /${vol}/${buck}/${KEY} /tmp/downloadedfile

    # Verify short circuit read metrics from datanode JMX
    # The metric is numLocalGetBlock
    ${jmx_output} =     Execute             curl -s 'http://localhost:9882/jmx?qry=Hadoop:service=HddsDatanode,name=StorageContainerMetrics' | grep -o '"numLocalGetBlock" : [0-9]*' | awk -F: '{print $2}' | tr -d ' '
    Should Be True      ${jmx_output} > 0
    
    # Clean up
    Execute             rm /tmp/testfile /tmp/downloadedfile
