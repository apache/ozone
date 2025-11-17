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
Documentation       Test for using sh commands with snapshots.
Library             OperatingSystem
Resource            ../ozone-lib/shell.robot
Resource            snapshot-setup.robot
Test Timeout        10 minutes

*** Variables ***
${SNAPSHOT_ONE}
${SNAPSHOT_TWO}
${SNAPSHOT_THREE}
${KEY_ONE}
${KEY_TWO}
${KEY_THREE}

*** Test Cases ***
Snapshot Creation
    Setup volume and bucket
    ${key_one} =            snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/hosts
    Set Suite Variable      ${KEY_ONE}          ${key_one}
    ${snapshot_one} =       Create snapshot     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_ONE}     ${snapshot_one}

Snapshot List
    ${result} =     Execute             ozone sh snapshot ls /${VOLUME}/${BUCKET}
                    Should contain      ${result}       ${SNAPSHOT_ONE}
                    Should contain      ${result}       SNAPSHOT_ACTIVE

Snapshot Diff
    ${key_two} =            snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/passwd
    Set Suite Variable      ${KEY_TWO}          ${key_two}
    ${key_three} =          snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/group
    Set Suite Variable      ${KEY_THREE}        ${key_three}
    ${snapshot_two} =       Create snapshot     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_TWO}     ${snapshot_two}
    ${snapshot_three} =     Create snapshot     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_THREE}     ${snapshot_three}
    ${result} =     Execute             ozone sh snapshot diff /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_TWO}
                    Should contain      ${result}       Snapshot diff job is IN_PROGRESS
    ${result} =     Execute             ozone sh snapshot diff /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_THREE}
                    Should contain      ${result}       Snapshot diff job is IN_PROGRESS

Snapshot Diff as JSON
    ${result} =     Execute             ozone sh snapshot diff --json /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_TWO}
                    Should contain      echo '${result}' | jq '.jobStatus'   DONE
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.volumeName'    ${VOLUME}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.bucketName'    ${BUCKET}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.fromSnapshot'  ${SNAPSHOT_ONE}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.toSnapshot'    ${SNAPSHOT_TWO}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.diffList | .[].sourcePath'    ${KEY_TWO}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.diffList | .[].sourcePath'    ${KEY_THREE}
    ${result} =     Execute             ozone sh snapshot diff --json /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_TWO}
                    Should contain      echo '${result}' | jq '.jobStatus'   DONE

List Snapshot Diff Jobs
    ${result} =     Execute             ozone sh snapshot listDiff /${VOLUME}/${BUCKET} --all-status
                    Should contain      ${result}        ${VOLUME}
                    Should contain      ${result}        ${BUCKET}
                    Should contain      ${result}        ${SNAPSHOT_ONE}
                    Should contain      ${result}        ${SNAPSHOT_TWO}
                    Should contain      ${result}        ${SNAPSHOT_THREE}
    ${result} =     Execute             ozone sh snapshot listDiff /${VOLUME}/${BUCKET} --all-status -l=1 | jq 'length'
                    Should contain      ${result}        1

Read Snapshot
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_TWO}       /etc/passwd
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_THREE}     /etc/group

Delete snapshot
    ${output} =         Execute           ozone sh snapshot delete /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE}
                        Should not contain      ${output}       Failed

    ${output} =         Execute            ozone sh snapshot ls /${VOLUME}/${BUCKET} | jq '[.[] | select(.name == "${SNAPSHOT_ONE}") | .snapshotStatus] | if length > 0 then .[] else "SNAPSHOT_DELETED" end'
                        Should contain   ${output}   SNAPSHOT_DELETED

