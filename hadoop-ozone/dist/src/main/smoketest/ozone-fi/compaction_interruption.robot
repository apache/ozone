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
Documentation       Test compaction interruption and restart scenarios using Byteman fault injection
Resource            ../commonlib.robot
Resource            BytemanKeywords.robot
Test Setup          Setup Test Environment
Test Teardown       Cleanup Test Environment

*** Variables ***
${COMPACTION_FAULT_RULE}    /opt/hadoop/share/ozone/byteman/compaction-fault-injection.btm
${VOLUME}                   compaction-test-vol
${BUCKET}                   compaction-test-bucket
${PREFIX}                   compaction-test-key

*** Keywords ***
Setup Test Environment
    Create volume      ${VOLUME}
    Create bucket      ${VOLUME}  ${BUCKET}
    # Enable compaction service with short interval
    Execute            ozone admin om setconfig ozone.om.compaction.service.enabled true
    Execute            ozone admin om setconfig ozone.om.compaction.service.run.interval 10s

Cleanup Test Environment
    # Remove all byteman rules from all components
    Remove All Rules From All Components
    # Clean up test data
    Run Keyword And Ignore Error    Delete bucket    ${VOLUME}  ${BUCKET}
    Run Keyword And Ignore Error    Delete volume    ${VOLUME}

Write Test Data
    [Arguments]    ${num_keys}=100
    [Documentation]    Write test data to trigger compaction
    FOR    ${i}    IN RANGE    ${num_keys}
        Execute    echo "test-data-${i}" | ozone sh key put ${VOLUME}/${BUCKET}/${PREFIX}-${i}
    END
    Log    Wrote ${num_keys} test keys to trigger compaction

Wait For Compaction Activity
    [Documentation]    Wait for compaction activity to start
    Sleep    15s    # Allow time for compaction service to trigger

Verify OM Service Status
    [Arguments]    ${expected_status}=running
    [Documentation]    Verify OM service status
    ${result} =    Execute    ozone admin om status
    Should Contain    ${result}    ${expected_status}

Restart OM Service
    [Documentation]    Restart OM service and wait for it to come back up
    Execute    supervisorctl restart om1
    Sleep    10s    # Wait for service to restart
    Verify OM Service Status    running

Check Compaction Logs
    [Documentation]    Check logs for compaction activity and fault injection
    ${logs} =    Execute    grep -i "compaction\|FAULT INJECTION" /var/log/hadoop/om*.log | tail -20
    Log    Compaction and fault injection logs:\n${logs}
    [Return]    ${logs}

*** Test Cases ***

Test Compaction Delay Injection
    [Documentation]    Test compaction delay using Byteman fault injection
    [Tags]    compaction    fault-injection    delay
    
    # Write initial data
    Write Test Data    50
    
    # Inject compaction delay fault into OM
    Add Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Write more data to trigger compaction
    Write Test Data    100
    
    # Wait for compaction activity with delays
    Wait For Compaction Activity
    
    # Check that fault injection is working
    ${logs} =    Check Compaction Logs
    Should Contain    ${logs}    FAULT INJECTION
    
    # Remove fault injection
    Remove Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Verify normal operation resumes
    Write Test Data    20
    Sleep    10s

Test Compaction Failure and Recovery
    [Documentation]    Test compaction failure and recovery scenarios
    [Tags]    compaction    fault-injection    failure    recovery
    
    # Write initial data
    Write Test Data    75
    
    # Inject compaction failure fault into OM
    Add Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Trigger compaction that should fail
    Write Test Data    150
    Wait For Compaction Activity
    
    # Check for fault injection in logs
    ${logs} =    Check Compaction Logs
    Should Contain    ${logs}    FAULT INJECTION
    
    # Remove fault injection to allow recovery
    Remove Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Wait for recovery
    Sleep    20s
    
    # Verify system recovers and compaction works
    Write Test Data    30
    Sleep    15s
    
    # Check recovery in logs
    ${recovery_logs} =    Check Compaction Logs
    Log    Recovery logs:\n${recovery_logs}

Test OM Restart During Compaction
    [Documentation]    Test OM restart while compaction is in progress
    [Tags]    compaction    restart    resilience
    
    # Write data to trigger compaction
    Write Test Data    200
    
    # Inject delay to make compaction longer running
    Add Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Start compaction activity
    Write Test Data    100
    Sleep    5s    # Let compaction start
    
    # Restart OM while compaction might be in progress
    Restart OM Service
    
    # Remove fault injection after restart
    Remove Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Verify OM is healthy after restart
    Verify OM Service Status    running
    
    # Verify compaction service recovers
    Write Test Data    50
    Sleep    20s
    
    # Check logs for restart and recovery
    ${logs} =    Check Compaction Logs
    Log    Restart and recovery logs:\n${logs}

Test Multiple OM Compaction Fault Injection
    [Documentation]    Test fault injection across multiple OM instances in HA setup
    [Tags]    compaction    fault-injection    ha    multiple-om
    
    # Write initial data
    Write Test Data    100
    
    # Inject faults into multiple OMs
    Inject Fault Into OMs Only    ${COMPACTION_FAULT_RULE}
    
    # Trigger compaction activity
    Write Test Data    200
    Wait For Compaction Activity
    
    # Check fault injection across OMs
    List Byteman Rules for OMs
    
    # Remove faults from all OMs
    Remove Fault From OMs Only    ${COMPACTION_FAULT_RULE}
    
    # Verify recovery across all OMs
    Sleep    15s
    Write Test Data    50

Test Compaction Service Resilience
    [Documentation]    Test compaction service resilience under various fault conditions
    [Tags]    compaction    resilience    stress
    
    # Initial data load
    Write Test Data    150
    
    # Test sequence: inject fault -> write data -> remove fault -> repeat
    FOR    ${iteration}    IN RANGE    3
        Log    Starting resilience test iteration ${iteration + 1}
        
        # Inject fault
        Add Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
        
        # Write data under fault condition
        Write Test Data    50
        Sleep    10s
        
        # Remove fault
        Remove Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
        
        # Allow recovery
        Sleep    10s
        
        # Write data under normal condition
        Write Test Data    25
        Sleep    5s
    END
    
    # Final verification
    ${logs} =    Check Compaction Logs
    Should Contain    ${logs}    FAULT INJECTION
    
    # Verify system is stable
    Verify OM Service Status    running

Test Compaction Interruption During Snapshot Operations
    [Documentation]    Test compaction interruption during snapshot operations
    [Tags]    compaction    snapshot    fault-injection
    
    # Write initial data
    Write Test Data    100
    
    # Create a snapshot
    Execute    ozone sh snapshot create ${VOLUME} ${BUCKET} snap1
    
    # Inject compaction fault
    Add Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Write more data to trigger compaction
    Write Test Data    150
    
    # Create another snapshot while compaction is affected
    Execute    ozone sh snapshot create ${VOLUME} ${BUCKET} snap2
    
    # Remove fault injection
    Remove Byteman Rule    om1    ${COMPACTION_FAULT_RULE}
    
    # Verify snapshots are intact
    ${snap1_info} =    Execute    ozone sh snapshot info ${VOLUME} ${BUCKET} snap1
    ${snap2_info} =    Execute    ozone sh snapshot info ${VOLUME} ${BUCKET} snap2
    Should Contain    ${snap1_info}    snap1
    Should Contain    ${snap2_info}    snap2
    
    # Cleanup snapshots
    Execute    ozone sh snapshot delete ${VOLUME} ${BUCKET} snap1
    Execute    ozone sh snapshot delete ${VOLUME} ${BUCKET} snap2 