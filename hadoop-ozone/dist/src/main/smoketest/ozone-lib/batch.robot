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
Documentation       Execute ozone commands in batch
Library             Collections
Library             OperatingSystem
Resource            ../lib/os.robot
Resource            ../ozone-lib/shell.robot


*** Keywords ***
Setup Batch
    Init Batch
    Set Suite Variable    ${BATCH_OUTPUT_FILE_COUNTER}    0
    ${BATCH_OUTPUT_DIR} =    Get Next Filename    ${TEMP_DIR}/batch
    Set Suite Variable    ${BATCH_OUTPUT_DIR}
    Create Directory      ${BATCH_OUTPUT_DIR}


Teardown Batch
    Execute Batch
    Remove Directory    ${BATCH_OUTPUT_DIR}    recursive=True


Init Batch
    Set Suite Variable    ${BATCH_SUBCMD}    ${EMPTY}
    Set Suite Variable    @{BATCH_COMMANDS}    @{EMPTY}
    Set Suite Variable    ${BATCH_QUEUEING}    ${FALSE}
    Set Suite Variable    @{BATCH_ASSERTIONS}    @{EMPTY}


# get unique name using ${BATCH_OUTPUT_FILE_COUNTER}
Get Next Filename
    [arguments]    ${prefix}
    ${i} =    Set Variable    ${BATCH_OUTPUT_FILE_COUNTER}
    ${i} =    Evaluate    ${i} + 1
    Set Suite Variable    ${BATCH_OUTPUT_FILE_COUNTER}    ${i}
    [return]    ${prefix}-${i}


# queue ozone command for batch execution
# return path where output will be available
Execute Later ozone ${subcmd}
    [arguments]    ${command}

    IF    '${BATCH_SUBCMD}' != '${EMPTY}' and '${subcmd}' != '${BATCH_SUBCMD}'
        Execute Batch
    END

    TRY
        ${test_name} =    Set Variable    ${TEST_NAME}
    EXCEPT
        ${test_name} =    Set Variable    Suite Setup
    END

    ${output_file} =    Get Next Filename    ${BATCH_OUTPUT_DIR}/out-ozone-${subcmd}
    ${cmd_with_redirection} =    Set Variable    ${command} > ${output_file}

    ${debug_comment} =    Set Variable    \# ${test_name}
    Append To List    ${BATCH_COMMANDS}    ${debug_comment}
    Append To List    ${BATCH_COMMANDS}    ${cmd_with_redirection}
    Set Suite Variable    ${BATCH_QUEUEING}    ${TRUE}
    Set Suite Variable    ${BATCH_SUBCMD}    ${subcmd}
    Log    Output of test ${test_name} command ${command} redirected to ${output_file}

    RETURN    ${output_file}


# queue keyword execution for batch execution after all tests
Assert Later
    [arguments]    @{assertion}

    IF    '${BATCH_QUEUEING}' == '${TRUE}'
        &{test_case} =   Create Dictionary    name=${TEST_NAME}    assertions=${assertion}
        Append To List    ${BATCH_ASSERTIONS}    ${test_case}
    ELSE
        Run Keyword    @{assertion}
    END


Execute Batch of Commands
    IF    '${BATCH_SUBCMD}' == '${EMPTY}'    RETURN
    Run Keyword    Ozone Batch ${BATCH_SUBCMD}    @{BATCH_COMMANDS}
    [teardown]    Set Suite Variable    @{BATCH_COMMANDS}    @{EMPTY}


Execute Batch of Assertions
    FOR    ${test_case}    IN    @{BATCH_ASSERTIONS}
        Log    Executing assertions for: ${test_case.name}
        FOR    ${a}    IN     ${test_case.assertions}
            TRY
                Run Keyword    @{a}
            EXCEPT
                Fail    ${test_case.name} failed at @{a}
            END
        END
    END
    [teardown]    Set Suite Variable    @{BATCH_ASSERTIONS}    @{EMPTY}


Execute Batch
    Execute Batch of Commands
    Execute Batch of Assertions
