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
Test Timeout        20 minutes

*** Variables ***
${SECURITY_ENABLED}                 false
${HOST}                             om1
${USERNAME}                         hadoop
${PUBLIC_KEY}                       /opt/.ssh/id_rsa
${OM_SERVICE_ID}                    %{OM_SERVICE_ID}
${OZONE_LOG_DIR}                    /ozone/logs/
${RATIS_DIR}                        /data/metadata/ratis
${VOLUME}                           volume1
${BUCKET}                           bucket1
${TEST_FILE}                        NOTICE.txt
${WRITE_FILE_COUNT}                 0
${TEMPDIR}                          /tmp

** Keywords ***
Open Connection And Log In
   Open Connection                  ${HOST}
   Login With Public Key            ${USERNAME}             ${PUBLIC_KEY}

Start OM
    [arguments]             ${OM_HOST}
                            Set Global Variable             ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
    ${rc1} =                Execute Command                 /opt/startOM.sh --restart       return_stdout=False    return_rc=True
                            Should Be Equal As Integers     ${rc1}                  0
    ${startMsg}  ${rc2} =   Execute Command                 sudo ps aux | grep om           return_rc=True
                            Should Be Equal As Integers     ${rc2}                  0
                            Close Connection
                            Should Contain                  ${startMsg}             OzoneManagerStarter

Stop OM
    [arguments]             ${OM_HOST}
                            Set Global Variable             ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
    ${rc1} =                Execute Command                 /opt/stopOM.sh                  return_stdout=False    return_rc=True
                            Should Be Equal As Integers     ${rc1}                  0
    ${stopMsg}  ${rc2} =    Execute Command                 sudo ps aux | grep om           return_rc=True
                            Should Be Equal As Integers     ${rc2}                  0
                            Close Connection
                            Should Not Contain              ${stopMsg}              OzoneManagerStarter

Create volume and bucket
    Execute                 ozone sh volume create o3://${OM_SERVICE_ID}/${VOLUME}
    Execute                 ozone sh bucket create o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}

Write Test File
    ${writeFileCount} =     Evaluate                ${WRITE_FILE_COUNT}+1
                            Set Global Variable     ${WRITE_FILE_COUNT}     ${writeFileCount}
    ${fileName} =           Set Variable            omha-${WRITE_FILE_COUNT}.txt
    ${testFilePath} =       Set Variable            ${TEMPDIR}/${fileName}
                            Copy File               ${TEST_FILE}            ${testFilePath}
                            Execute                 ozone fs -copyFromLocal ${testFilePath} o3fs://${BUCKET}.${VOLUME}.${OM_SERVICE_ID}/
    ${result} =             Execute                 ozone sh key list o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} | jq -r '.name'
                            Should contain          ${result}               ${fileName}
                            Remove File             ${testFilePath}

Put Key
    [arguments]             ${FILE}                 ${KEY}
                            Execute                 ozone sh key put o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${KEY} ${FILE}
    ${result} =             Execute                 ozone sh key info o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET}/${KEY} | jq -r '. | select(.name=="${KEY}")'
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
    ${groupId}   ${rc} =    Execute Command         ls -tr ${RATIS_DIR} | grep -v 'snapshot'     return_rc=True
                            Should Be Equal As Integers                     ${rc}                0
                            LOG                     Ratis GroupId: ${groupId}
    ${currDir} =            Catenate                SEPARATOR=              ${RATIS_DIR}    /    ${groupId}    /current/
    @{logs} =               SSHLibrary.List Files In Directory              ${currDir}           log_[0-9]*
                            Close Connection
    [return]                ${logs}

Get Last Log Index
    [arguments]             ${logs}
    @{endLogIndices} =      Create List
    :FOR       ${log}       IN      @{logs}
    \          ${endIndex} =        Fetch From Right        ${log}          -
    \          Append To List       ${endLogIndices}        ${endIndex}

    ${listSize} =           Get Length          ${endLogIndices}
    ${lastLogIndex} =       Run Keyword If      ${listSize} == 0        Set Variable        0       ELSE        Get Max Element From List       ${endLogIndices}
    [return]                ${lastLogIndex}

Get First Log Index
    [arguments]             ${logs}
    @{startLogIndices} =    Create List
    :FOR       ${log}       IN      @{logs}
    \          ${indexRange} =      Fetch From Right        ${log}          _
    \          ${startIndex} =      Fetch From Left         ${indexRange}   -
    \          Append To List       ${startLogIndices}        ${startIndex}

    ${listSize} =           Get Length          ${startLogIndices}
    ${firstLogIndex} =      Run Keyword If      ${listSize} == 0        Set Variable        0       ELSE        Get Min Element From List       ${startLogIndices}
    [return]                ${firstLogIndex}

Get Min Element From List
    [arguments]             ${list}
    ${listSize} =           Get Length              ${list}
    ${min} =                Get From List           ${list}                 0
    FOR        ${INDEX}     IN RANGE                ${listSize}
               ${num} =     Get From List           ${list}                 ${INDEX}
               ${min} =     Run Keyword If          ${num} < ${min}         Set Variable        ${num}     ELSE     Set Variable        ${min}
    END
    [return]                ${min}

Get Max Element From List
    [arguments]             ${list}
    ${listSize} =           Get Length              ${list}
    ${max} =                Get From List           ${list}                 0
    FOR        ${INDEX}     IN RANGE                ${listSize}
               ${num} =     Get From List           ${list}                 ${INDEX}
               ${max} =     Run Keyword If          ${num} > ${max}         Set Variable        ${num}     ELSE     Set Variable        ${max}
    END
    [return]                ${max}

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

Restart OM and Verify Ratis Logs
    Set Test Variable       ${OM_HOST}              om2
    Set Test Variable       ${keyBase}              testOMRestart_
    Set Test Variable       ${keyBaseAfterRestart}  testOMRestart_afterRestart_

    # Stop 1 OM and get the Logs present in its Ratis Dir
                                    Stop OM                        ${OM_HOST}
    ${logs}                         Get Ratis Logs                 ${OM_HOST}
    ${lastLogBeforeShutdown} =      Get Last Log Index             ${logs}

    # Get the current leader OM
    ${leaderOM} =                   Get OM Leader Node

    # Perform write operations to advance the Ratis log index till a new Log segment is created
    # For this we check that the last log index on leader is atleast 10 more than the last log index on follower before shutdown
    ${nextLogIndex} =               Evaluate                    ${lastLogBeforeShutdown} + 10
    FOR     ${INDEX}    IN RANGE    50
            Set Test Variable       ${keyPrefix}                ${keyBase}${INDEX}
            Put Multiple Keys       5                           ${keyPrefix}            ${TEST_FILE}
            ${leaderLogs}           Get Ratis Logs              ${leaderOM}
            ${lastLogIndexLeader}   Get Last Log Index          ${leaderLogs}
            EXIT FOR LOOP IF        ${lastLogIndexLeader} >= ${nextLogIndex}
    END
    Should Be True                  ${lastLogIndexLeader} > ${nextLogIndex}             Cannot test OM Restart as Ratis did not start new log segment.

    # Restart the stopped OM and wait for Ratis to catch up with Leader OM
            Start OM                ${OM_HOST}
    FOR     ${INDEX}    IN RANGE    300
            ${followerLogs}         Get Ratis Logs              ${OM_HOST}
            ${lastLogAfterRestart}  Get Last Log Index          ${followerLogs}
            EXIT FOR LOOP IF        ${lastLogAfterRestart} >= ${lastLogIndexLeader}
            Sleep                   1s
    END
    Should Be True                  ${lastLogAfterRestart} >= ${lastLogIndexLeader}     Restarted OM did not catch up with Leader OM

    # Verify that the last log index of restarted follower is greater than equal to the last log index of the leader
    # before the restart. Perform some put key operations to verify that the follower log index advances.
    FOR     ${INDEX}    IN RANGE    10
            Set Test Variable       ${keyPrefix}                ${keyBaseAfterRestart}${INDEX}
            Put Multiple Keys       5                           ${keyPrefix}            ${TEST_FILE}
            ${followerLogs}         Get Ratis Logs              ${OM_HOST}
            ${lastLogAfterRestart}  Get Last Log Index          ${followerLogs}
            EXIT FOR LOOP IF        ${lastLogAfterRestart} >= ${lastLogIndexLeader}
    END
    Should Be True                  ${lastLogAfterRestart} >= ${lastLogIndexLeader}     Restarted Follower not participating in Ratis Ring

Test Install Ratis Snapshot
    Set Test Variable       ${OM_HOST}              om2
    Set Test Variable       ${keyBase}              testOMInstallSnapshot_
    Set Test Variable       ${testKeyFinal}         testOMInstallSnapshot_testKeyFinal

    # Stop 1 OM and get the Logs present in its Ratis Dir
                                    Stop OM                        ${OM_HOST}
    ${logs}                         Get Ratis Logs                 ${OM_HOST}
    ${lastLogBeforeShutdown} =      Get Last Log Index             ${logs}

    # Get the current leader OM
    ${leaderOM} =                   Get OM Leader Node

    # Perform write operations to advance the Ratis log index till previous logs are purged.
    # To ensure that on restart OM receives a snapshot, we wait till the leader has purged logs upto follower next
    # index + 10.
    ${nextLogIndexAfterPurge} =     Evaluate                    ${lastLogBeforeShutdown} + 10
    FOR     ${INDEX}    IN RANGE    100
            Set Test Variable       ${keyPrefix}                ${keyBase}${INDEX}
            Put Multiple Keys       5                           ${keyPrefix}            ${TEST_FILE}
            ${leaderLogs}           Get Ratis Logs              ${leaderOM}
            ${firstLogIndex}        Get First Log Index         ${leaderLogs}
            EXIT FOR LOOP IF        ${firstLogIndex} > ${nextLogIndexAfterPurge}
    END
    Should Be True                  ${firstLogIndex} > ${nextLogIndexAfterPurge}        Cannot test OM Install Snapshot as Ratis logs were not purged.
    ${numLogsLeader}                Get Length                  ${leaderLogs}

    # Restart the stopped OM. It should catch up with Leader OM by installing snapshot from the Leader.
            Start OM                ${OM_HOST}
    FOR     ${INDEX}    IN RANGE    300
            ${followerLogs}         Get Ratis Logs              ${OM_HOST}
            ${lastLogAfterRestart}  Get Last Log Index          ${followerLogs}
            EXIT FOR LOOP IF        ${lastLogAfterRestart} > ${lastLogBeforeShutdown}
            Sleep                   100ms
    END
    Should Be True                  ${lastLogAfterRestart} > ${lastLogBeforeShutdown}   Restarted OM did not catch up with Leader OM

    # To test that restarted OM is caught up and participating in the quorum, stop another OM and try a Put Key
            Stop OM                 om3
            Put Key                 ${TEST_FILE}                ${testKeyFinal}
            Start OM                om3

