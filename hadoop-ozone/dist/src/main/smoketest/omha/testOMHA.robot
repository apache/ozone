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
Test Timeout        8 minutes

*** Variables ***
${SECURITY_ENABLED}                 false
${HOST}                             om1
${USERNAME}                         hadoop
${PUBLIC_KEY}                       /opt/.ssh/id_rsa
${OM_SERVICE_ID}                    %{OM_SERVICE_ID}
${OZONE_LOG_DIR}                    /ozone/logs/
${RATIS_DIR}                        /data/metadata/om.ratis
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
    ${result} =             Execute                 ozone sh key list o3://${OM_SERVICE_ID}/${VOLUME}/${BUCKET} | jq -r '.[].name'
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

Get Ratis Logs
    [arguments]             ${OM_HOST}
                            Set Global Variable     ${HOST}                 ${OM_HOST}
                            Open Connection And Log In
    ${groupId}   ${rc} =    Execute Command         ls ${RATIS_DIR} | grep -v 'snapshot'         return_rc=True
                            Should Be Equal As Integers                     ${rc}                0
                            LOG                     Ratis GroupId: ${groupId}
    ${currDir} =            Catenate                SEPARATOR=              ${RATIS_DIR}    /    ${groupId}    /current/
    @{logs} =               SSHLibrary.List Files In Directory              ${currDir}           log_*
                            Close Connection
    ${numLogs} =            Get Length                                      ${logs}
    [return]                ${numLogs}                                      ${logs}

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