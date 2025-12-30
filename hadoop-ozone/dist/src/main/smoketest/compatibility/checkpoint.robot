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
Documentation       Checkpoint Compatibility
Resource            ../ozone-lib/shell.robot
Resource            setup.robot
Test Timeout        5 minutes

*** Variables ***
${CHECKPOINT_V2_VERSION}    2.1.0
${OM_HOST}                  om
${OM_PORT}                  9874

*** Keywords ***
Download Checkpoint V1
    [Documentation]    Download checkpoint using v1 endpoint (/dbCheckpoint)
    [Arguments]        ${expected_result}
    
    Log                   Testing v1 checkpoint endpoint with authentication
    
    # Try different keytabs based on client version/container
    ${download_file} =    Set Variable    /tmp/checkpoint_v1_${CLIENT_VERSION}.tar.gz
    
    # Debug: Check keytab availability first
    ${keytab_check} =     Execute    ls -la /etc/security/keytabs/ 2>&1 | head -5 || echo "No keytabs directory"
    Log                   Keytab directory: ${keytab_check}
    
    # Combine kinit and curl in a single command to preserve Kerberos session
    ${combined_cmd} =     Set Variable    kinit -k -t /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM && curl -f --negotiate -u : --connect-timeout 10 --max-time 30 -o ${download_file} http://${OM_HOST}:${OM_PORT}/dbCheckpoint
    
    Log                   Executing: ${combined_cmd}
    ${result} =           Execute and checkrc    ${combined_cmd}    ${expected_result}
    
    IF    ${expected_result} == 0
        # If we expect success, verify the file was created and has content
        ${file_check} =    Execute    ls -la ${download_file} 2>/dev/null || echo "File not found"
        Should Not Contain    ${file_check}    File not found
        Should Contain        ${file_check}    checkpoint_v1_${CLIENT_VERSION}.tar.gz
        Log                   Successfully downloaded checkpoint via v1 endpoint: ${file_check}
    ELSE
        Log                   v1 endpoint failed as expected for this version combination
    END

Download Checkpoint V2
    [Documentation]    Download checkpoint using v2 endpoint (/dbCheckpointv2)
    [Arguments]        ${expected_result}
    
    Log                   Testing v2 checkpoint endpoint with authentication
    
    # Debug: Check keytab availability first (reuse from V1 if already checked)
    ${keytab_check} =     Execute    ls -la /etc/security/keytabs/ 2>&1 | head -5 || echo "No keytabs directory"
    Log                   Keytab directory: ${keytab_check}
    
    # Combine kinit and curl in a single command to preserve Kerberos session
    ${download_file} =    Set Variable    /tmp/checkpoint_v2_${CLIENT_VERSION}.tar.gz
    ${combined_cmd} =     Set Variable    kinit -k -t /etc/security/keytabs/testuser.keytab testuser/scm@EXAMPLE.COM && curl -f --negotiate -u : --connect-timeout 10 --max-time 30 -o ${download_file} http://${OM_HOST}:${OM_PORT}/v2/dbCheckpoint
    
    Log                   Executing: ${combined_cmd}
    ${result} =           Execute and checkrc    ${combined_cmd}    ${expected_result}
    
    IF    ${expected_result} == 0
        # If we expect success, verify the file was created and has content
        ${file_check} =    Execute    ls -la ${download_file} 2>/dev/null || echo "File not found"
        Should Not Contain    ${file_check}    File not found
        Should Contain        ${file_check}    checkpoint_v2_${CLIENT_VERSION}.tar.gz
        Log                   Successfully downloaded checkpoint via v2 endpoint: ${file_check}
    ELSE
        Log                   v2 endpoint failed as expected for this version combination
    END

*** Test Cases ***
Checkpoint V1 Endpoint Compatibility
    [Documentation]    Test v1 checkpoint endpoint (/dbCheckpoint) - should work for all versions (backward compatibility)
    
    Log    Testing v1 checkpoint endpoint: CLIENT=${CLIENT_VERSION}, CLUSTER=${CLUSTER_VERSION}
    
    # Both old and new clusters should serve v1 endpoint for backward compatibility
    Download Checkpoint V1    0

Checkpoint V2 Endpoint Compatibility
    [Documentation]    Test v2 checkpoint endpoint (/v2/dbCheckpoint) - should only work with new cluster
    
    Log    Testing v2 checkpoint endpoint: CLIENT=${CLIENT_VERSION}, CLUSTER=${CLUSTER_VERSION}
    
    IF    '${CLUSTER_VERSION}' < '${CHECKPOINT_V2_VERSION}'
        # Old cluster doesn't have v2 endpoint - should fail with any non-zero exit code
        ${result} =    Run Keyword And Return Status    Download Checkpoint V2    0
        IF    not ${result}
            Log    v2 endpoint correctly failed on old cluster ${CLUSTER_VERSION} (expected failure)
        ELSE
            Fail    v2 endpoint unexpectedly succeeded on old cluster ${CLUSTER_VERSION}
        END
    ELSE
        # New cluster has v2 endpoint - should succeed
        Download Checkpoint V2    0
        Log    v2 endpoint correctly succeeded on new cluster ${CLUSTER_VERSION}
    END
