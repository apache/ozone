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
Documentation       Test ozone debug datanode container checksum command
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ../lib/string.robot
Resource            ../commonlib.robot
Test Timeout        5 minute
Suite Setup         Setup Container Checksum Tests

*** Variables ***
${VOLUME}           container-checksum-volume
${BUCKET}           container-checksum-bucket
${TESTFILE}         container-test-file
${CONTAINER_DIR}    ${EMPTY}
${TREE_FILE}        ${EMPTY}

*** Keywords ***
Setup Container Checksum Tests
    # Create volume and bucket with some data (ignore if already exists)
    Execute and ignore error    ozone sh volume create ${VOLUME}
    Execute and ignore error    ozone sh bucket create ${VOLUME}/${BUCKET}
    Execute             dd if=/dev/urandom of=${TEMP_DIR}/${TESTFILE} bs=10000 count=10
    Execute and ignore error    ozone sh key put ${VOLUME}/${BUCKET}/${TESTFILE} ${TEMP_DIR}/${TESTFILE}
    
    # Find a container directory on one of the datanodes
    ${output} =                 Execute                 find /data -name "*.container" -type f 2>/dev/null | head -1
    ${container_file} =         Strip String            ${output}
    ${matches} =                Get Regexp Matches      ${container_file}    ^(.*/containers/.*)/.*.container$    1
    ${CONTAINER_DIR} =          Set Variable If         ${matches}    ${matches}[0]    ${EMPTY}
    Set Suite Variable          ${CONTAINER_DIR}
    
    # Find a tree file in the container directory
    Run Keyword If              '${CONTAINER_DIR}' != '${EMPTY}'    Find Tree File
    Set Suite Variable          ${TREE_FILE}

Find Tree File
    ${tree_file} =              Execute                 find ${CONTAINER_DIR} -name "*.tree" 2>/dev/null | head -1
    ${TREE_FILE} =              Strip String            ${tree_file}
    Set Suite Variable          ${TREE_FILE}

*** Test Cases ***
Test container checksum command with missing arguments
    ${result} =         Execute and checkrc    ozone debug datanode container checksum    2
                        Should Contain    ${result}    Missing required parameter

Test container checksum command with too many arguments
    Pass Execution If   '${TREE_FILE}' == '${EMPTY}'    No tree file found, skipping test
    ${result} =         Execute and checkrc    ozone debug datanode container checksum ${TREE_FILE} extra_arg    2
                        Should Contain    ${result}    Unmatched argument

Test container checksum command displays JSON output
    Pass Execution If   '${TREE_FILE}' == '${EMPTY}'    No tree file found, skipping test
    ${output} =         Execute    ozone debug datanode container checksum ${TREE_FILE}
    # Validate it's valid JSON and check structure
    ${json_output} =    Execute    echo '${output}' | jq -r '.'
                        Should Contain    ${json_output}    containerID
                        Should Contain    ${json_output}    containerMerkleTree
    # Verify containerID is a number
    ${container_id} =   Execute    echo '${output}' | jq -r '.containerID'
                        Should Match Regexp    ${container_id}    ^[0-9]+$
    # Verify blockMerkleTrees exists if containerMerkleTree is present
    ${has_tree} =       Execute and checkrc    echo '${output}' | jq -e '.containerMerkleTree' > /dev/null 2>&1    0    true
    Run Keyword If      '${has_tree}' == 'PASS'    Verify Block Merkle Trees    ${output}

Test container checksum command with non-existent file
    ${result} =         Execute and checkrc    ozone debug datanode container checksum /non/existent/file.tree    255
                        Should Contain    ${result}    does not exist

Test container checksum command validates tree file format
    # Create a temporary invalid tree file
    Execute             echo "invalid data" > ${TEMP_DIR}/invalid.tree
    ${result} =         Execute and checkrc    ozone debug datanode container checksum ${TEMP_DIR}/invalid.tree    255
                        Should Contain    ${result}    Failed to read tree file

Test container checksum command verifies block structure
    Pass Execution If   '${TREE_FILE}' == '${EMPTY}'    No tree file found, skipping test
    ${output} =         Execute    ozone debug datanode container checksum ${TREE_FILE}
    # Check if blockMerkleTrees is present and is an array
    ${has_blocks} =     Execute and checkrc    echo '${output}' | jq -e '.containerMerkleTree.blockMerkleTrees | type == "array"' > /dev/null 2>&1    0    true
    Run Keyword If      '${has_blocks}' == 'PASS'    Verify First Block Structure    ${output}

*** Keywords ***
Verify Block Merkle Trees
    [Arguments]         ${json_output}
    ${blocks} =         Execute    echo '${json_output}' | jq -r '.containerMerkleTree.blockMerkleTrees | type'
                        Should Be Equal    ${blocks}    array

Verify First Block Structure
    [Arguments]         ${json_output}
    # Get the first block if exists
    ${block_count} =    Execute    echo '${json_output}' | jq -r '.containerMerkleTree.blockMerkleTrees | length'
    Return From Keyword If    '${block_count}' == '0'
    # Verify first block has required fields
    ${has_block_id} =   Execute and checkrc    echo '${json_output}' | jq -e '.containerMerkleTree.blockMerkleTrees[0].blockID' > /dev/null 2>&1    0    true
                        Should Be Equal    ${has_block_id}    PASS
    ${has_deleted} =    Execute and checkrc    echo '${json_output}' | jq -e '.containerMerkleTree.blockMerkleTrees[0].deleted | type == "boolean"' > /dev/null 2>&1    0    true
                        Should Be Equal    ${has_deleted}    PASS
    ${has_checksum} =   Execute and checkrc    echo '${json_output}' | jq -e '.containerMerkleTree.blockMerkleTrees[0].dataChecksum' > /dev/null 2>&1    0    true
                        Should Be Equal    ${has_checksum}    PASS

