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
Documentation       Smoketest ozone cluster startup
Library             OperatingSystem
Library             SSHLibrary
Library             Collections
Resource            ../commonlib.robot

*** Variables ***
${SECURITY_ENABLED}                 false
${HOST}                             om1
${USERNAME}                         hadoop
${PUBLIC_KEY}                       /opt/.ssh/id_rsa
${OM_SERVICE_ID}                    omservice
${OZONE_LOG_DIR}                    /ozone/logs/
${RATIS_DIR}                        /data/metadata/ratis
${VOLUME}                           volume1
${BUCKET}                           bucket1
${TEST_FILE}                        NOTICE.txt
${WRITE_FILE_COUNT}                 0

** Keywords ***
Open Connection And Log In
   Open Connection                  ${HOST}
   Login With Public Key            ${USERNAME}             ${PUBLIC_KEY}

Start OM
    [arguments]             ${OM_HOST}
                            Set Global Variable             ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
                            Execute Command                 /opt/startOM.sh --restart
    ${startupMsg} =         Execute Command                 sudo ps aux | grep om
                            Close Connection
                            Should Contain                  ${startupMsg}           OzoneManagerStarter

Stop OM
    [arguments]             ${OM_HOST}
                            Set Global Variable             ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
                            Execute Command                 /opt/stopOM.sh
    ${shutdownMsg} =        Execute Command                 sudo ps aux | grep om
                            Close Connection
                            Should Not Contain              ${shutdownMsg}          OzoneManagerStarter

Create volume and bucket
    Execute                 ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Execute                 ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}

Write Test File
    ${writeFileCount} =     Evaluate                ${WRITE_FILE_COUNT}+1
                            Set Global Variable     ${WRITE_FILE_COUNT}     ${writeFileCount}
    ${fileName} =           Catenate                SEPARATOR=              ${WRITE_FILE_COUNT}       .txt
                            Copy File               ${TEST_FILE}            ${fileName}
                            Execute                 ozone fs -copyFromLocal ${fileName} o3fs://${BUCKET}.${VOLUME}.${OM_SERVICE_ID}/
    ${result} =             Execute                 ozone sh key list o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} | grep -v WARN | jq -r '.name'
                            Should contain          ${result}               ${fileName}
                            Remove File             ${fileName}

Put Key
    [arguments]             ${FILE}                 ${KEY}
                            Execute                 ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${KEY} ${FILE}
    ${result} =             Execute                 ozone sh key info o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${KEY} | grep -Ev 'Removed|WARN|DEBUG|ERROR|INFO|TRACE' | jq -r '. | select(.name=="${KEY}")'
                            Should contain          ${result}               creationTime

Put Multiple Keys
    [arguments]             ${NUM_KEYS}             ${KEY_PREFIX}           ${FILE}
    FOR     ${INDEX}        IN RANGE                ${NUM_KEYS}
            ${tmpKey} =     Catenate                SEPARATOR=              ${KEY_PREFIX}       _       ${INDEX}
            LOG             ${tmpKey}
            Put Key         ${FILE}                 ${tmpKey}
    END

Get OM Leader Node
    ${result} =             Execute                 ozone admin om getserviceroles --service-id=omservice
                            LOG                     ${result}
                            Should Contain          ${result}               LEADER              1
                            Should Contain          ${result}               FOLLOWER            2
    ${omLine} =             Get Lines Containing String                     ${result}           LEADER
    ${split1}               ${split2}               Split String            ${omLine}           :
    ${leaderOM} =           Strip String            ${split1}
                            LOG                     Leader OM: ${leaderOM}
    [return]                ${leaderOM}

Get Ratis Logs
    [arguments]             ${OM_HOST}
                            Set Global Variable     ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
    ${gorupId} =            Execute Command         ls ${RATIS_DIR} | grep -v 'snapshot'
                            LOG                     Ratis GroupId: ${gorupId}
    ${currDir} =            Catenate                SEPARATOR=              ${RATIS_DIR}    /    ${gorupId}    /current/
    @{logs} =               SSHLibrary.List Files In Directory              ${currDir}           log_*
    ${numLogs} =            Get Length                          ${logs}
    [return]                ${numLogs}                          ${logs}

** Test Cases ***
Stop Leader OM and Verify Failover
    # Check OM write operation before failover
    Create volume and bucket
    Write Test File

    # Find Leader OM and stop it
    ${leaderOM} =           Get OM Leader Node
    ${stopOMResult} =       Stop OM                 ${leaderOM}

    # Verify that new Leader OM is elected
    ${newLeaderOM} =        Get OM Leader Node
                            Should Not be Equal     ${leaderOM}       ${newLeaderOM}      OMs did not failover

    # Verify write succeeds after failover
    Write Test File

    # Restart stopped OM
    Start OM                ${leaderOM}

Restart OM and Verify Ratis Logs
    Set Test Variable       ${OM_HOST}              om2
    Set Test Variable       ${keyBase}              testOMRestart_

    # Stop 1 OM and get the Logs present in its Ratis Dir
                            Stop OM                 ${OM_HOST}
    ${numLogsBefore}        @{logsBefore} =         Get Ratis Logs          ${OM_HOST}
    ${leaderOM} =           Get OM Leader Node

    # Perform write operations to advance the Ratis log index till a new Log segment is created
    FOR     ${INDEX}    IN RANGE    20
            Set Test Variable       ${keyPrefix}        ${keyBase}${INDEX}
            Put Multiple Keys       5                   ${keyPrefix}            ${TEST_FILE}
            ${numLogsLeader}        @{logsLeader} =     Get Ratis Logs          ${leaderOM}
            EXIT FOR LOOP IF        ${numLogsLeader} > ${numLogsBefore}
    END
    Should Be True                  ${numLogsLeader} > ${numLogsBefore}         Cannot test OM Restart as Ratis did not start new log segment.

    # Restart the stopped OM and wait for Ratis to catch up with Leader OM
            Start OM                ${OM_HOST}
    FOR     ${INDEX}    IN RANGE    300
            ${numLogsAfter}         @{logsAfter} =      Get Ratis Logs          ${OM_HOST}
            EXIT FOR LOOP IF        ${numLogsAfter} >= ${numLogsLeader}
            Sleep                   1s
    END
    Should Be True                  ${numLogsAfter} >= ${numLogsLeader}         Restarted OM did not catch up with Leader OM

    # Verify that the logs match with the Leader OMs logs
    List Should Contain Sub List    ${logsAfter}        ${logsLeader}

Test Multiple Failovers
    FOR     ${INDEX}    IN RANGE    5
            # Find Leader OM and stop it
            ${leaderOM} =               Get OM Leader Node
            ${stopOMResult} =           Stop OM                 ${leaderOM}

            # Verify that new Leader OM is elected
            ${newLeaderOM} =            Get OM Leader Node
            Should Not be Equal         ${leaderOM}             ${newLeaderOM}      OMs did not failover

            # Verify write succeeds after failover
            Write Test File

            # Restart OM
            Start OM                    ${leaderOM}
    END


