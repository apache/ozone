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
Documentation       Basic checks that snapshots still look correct while the OM runs periodic
...                 snapshot defrag in the background (Jira HDDS-15181 / parent HDDS-13003).
...                 Cluster setup: filesystem snapshots on; defrag interval in compose/ozone
...                 docker-config. The unsecure compose test.sh uses start_docker_env, which starts
...                 three datanodes by default; test.sh sets OZONE_REPLICATION_FACTOR=3. This suite
...                 should be run with execute_robot_test om: Robot runs inside the OM container and
...                 Snapshot Local YAML checks read paths under /data/metadata on that OM host.
Force Tags          om_filesystem
Library             OperatingSystem
Resource            ../ozone-lib/shell.robot
Resource            snapshot-setup.robot
Suite Setup         Run Keywords    Assert Snapshot Defrag Interval Is Positive
...                 AND    Detect Rocks Tools Available
...                 AND    Prepare Suite With Bucket And First Snapshot
Test Timeout        20 minutes

*** Variables ***
${DEFRAG_POLL_TIMEOUT}       10 minutes
${DEFRAG_POLL_INTERVAL}      5 seconds
${DEFRAG_FALLBACK_SECONDS}    65

*** Test Cases ***
Read Snapshot Data Right After Create
    [Documentation]     You can read the snapshotted key from the .snapshot path as soon as the snapshot exists.
    Key Should Match Local File         ${SNAP_KEY_PATH_ONE}       /etc/hosts

After Waiting Keys Still Match Through Snapshot And On Live Bucket
    [Documentation]     Add a new key on the live bucket, wait so defrag may run, then confirm the snapshot
    ...                 still has the old file and the live bucket has the new one.
    ${key_two} =            snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/passwd
    Set Suite Variable      ${KEY_TWO}          ${key_two}
    Set Suite Variable      ${LIVE_KEY_TWO_PATH}                   /${VOLUME}/${BUCKET}/${key_two}
    Wait Until Snapshot Local YAML Shows Defragged    ${SNAPSHOT_ONE}
    Key Should Match Local File         ${SNAP_KEY_PATH_ONE}       /etc/hosts
    Key Should Match Local File         ${LIVE_KEY_TWO_PATH}       /etc/passwd

Snapshot List Still Shows Active
    [Documentation]     ozone sh snapshot ls still lists this snapshot as SNAPSHOT_ACTIVE.
    ${result} =     Execute             ozone sh snapshot ls /${VOLUME}/${BUCKET}
                    Should contain      ${result}       ${SNAPSHOT_ONE}
                    Should contain      ${result}       SNAPSHOT_ACTIVE

Second Snapshot Sees All Keys So Far
    [Documentation]     Take another snapshot after adding a third key; older snapshot still only has the first key;
    ...                 newer snapshot can read all three keys.
    ${key_three} =            snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/group
    Set Suite Variable      ${KEY_THREE}        ${key_three}
    ${snapshot_two} =       Create snapshot                     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_TWO}     ${snapshot_two}
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_TWO}       /etc/passwd
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_THREE}     /etc/group

Snapshot Diff Starts A New Job
    [Documentation]     Comparing the two snapshots prints the usual “new job” and --get-report hint (like snapshot-sh.robot).
    ${result} =     Execute             ozone sh snapshot diff /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_TWO}
                    Should contain      ${result}       Submitting a new job
                    Should contain      ${result}       --get-report option

Snapshot Diff Json Report Lists Added Keys
    [Documentation]     Full JSON report finishes with DONE and lists the keys that appeared after the first snapshot.
    ${result} =     Execute             ozone sh snapshot diff --get-report --json /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE} ${SNAPSHOT_TWO}
                    Should contain      echo '${result}' | jq '.jobStatus'   DONE
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.volumeName'    ${VOLUME}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.bucketName'    ${BUCKET}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.fromSnapshot'  ${SNAPSHOT_ONE}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.toSnapshot'    ${SNAPSHOT_TWO}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.diffList | .[].sourcePath'    ${KEY_TWO}
                    Should contain      echo '${result}' | jq '.snapshotDiffReport.diffList | .[].sourcePath'    ${KEY_THREE}

After More Defrag Time Snapshot Info And Reads Stay Consistent
    [Documentation]     Poll OmSnapshot local YAML until defrag is recorded (version > 0, needsDefrag false),
    ...                 then re-check ozone sh snapshot info and snapshot reads. We do not rerun snapshot
    ...                 diff --get-report here: a completed diff report is served from cache for
    ...                 ozone.om.snapshot.diff.job.report.persistent.time, so that call would not retrigger work.
    Wait Until Snapshot Local YAML Shows Defragged      ${SNAPSHOT_ONE}
    Wait Until Snapshot Local YAML Shows Defragged      ${SNAPSHOT_TWO}
    ${info_one} =       Execute             ozone sh snapshot info /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE}
                        Should contain      echo '${info_one}' | jq '.volumeName'       ${VOLUME}
                        Should contain      echo '${info_one}' | jq '.bucketName'       ${BUCKET}
                        Should contain      echo '${info_one}' | jq '.name'             ${SNAPSHOT_ONE}
                        Should contain      echo '${info_one}' | jq '.snapshotStatus'   SNAPSHOT_ACTIVE
    ${snap_id_one} =    Execute             echo '${info_one}' | jq -r '.snapshotId'
                        Should contain      echo '${info_one}' | jq -r '.checkpointDir'    ${snap_id_one}
    ${info_two} =       Execute             ozone sh snapshot info /${VOLUME}/${BUCKET} ${SNAPSHOT_TWO}
                        Should contain      echo '${info_two}' | jq '.volumeName'       ${VOLUME}
                        Should contain      echo '${info_two}' | jq '.bucketName'       ${BUCKET}
                        Should contain      echo '${info_two}' | jq '.name'             ${SNAPSHOT_TWO}
                        Should contain      echo '${info_two}' | jq '.snapshotStatus'   SNAPSHOT_ACTIVE
    ${snap_id_two} =    Execute             echo '${info_two}' | jq -r '.snapshotId'
                        Should contain      echo '${info_two}' | jq -r '.checkpointDir'    ${snap_id_two}
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_ONE}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_TWO}       /etc/passwd
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_THREE}     /etc/group

Delete Older Snapshot Younger One Still Readable
    [Documentation]     Delete the first snapshot; it shows SNAPSHOT_DELETED; read all keys through the second snapshot path.
    ${output} =         Execute           ozone sh snapshot delete /${VOLUME}/${BUCKET} ${SNAPSHOT_ONE}
                        Should not contain                    ${output}       Failed
    ${output} =         Execute            ozone sh snapshot ls /${VOLUME}/${BUCKET} | jq --arg n '${SNAPSHOT_ONE}' '[.[] | select(.name == $n) | .snapshotStatus] | if length > 0 then .[] else "SNAPSHOT_DELETED" end'
                        Should contain                        ${output}       SNAPSHOT_DELETED
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_ONE}       /etc/hosts
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_TWO}       /etc/passwd
    Key Should Match Local File         /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${SNAPSHOT_TWO}/${KEY_THREE}     /etc/group

*** Keywords ***
Assert Snapshot Defrag Interval Is Positive
    [Documentation]     Default ozone.snapshot.defrag.service.interval is -1 (service off). This suite needs a
    ...                 positive interval (compose/ozone docker-config).
    ${ival} =    Execute    ozone getconf confKey ozone.snapshot.defrag.service.interval
    ${ival} =    Strip String    ${ival}
    Should Not Contain    ${ival}    -1
    Should Not Be Equal As Strings    ${ival}    ${EMPTY}

Detect Rocks Tools Available
    [Documentation]     SnapshotDefragService needs rocks-tools JNI. CI Linux dist embeds .so; a Mac-built jar may only
    ...                 contain .dylib, so defrag never runs and YAML version stays 0.
    ${out} =    Execute    ozone debug checknative
    ${ok} =     Run Keyword And Return Status    Should Match Regexp    ${out}    (?m)^\\s*rocks-tools:\\s+true\\b
    Set Suite Variable    ${ROCKS_TOOLS_AVAILABLE}    ${ok}

Get Snapshot Local YAML Path
    [Arguments]    ${snapshot_name}
    ${info} =          Execute    ozone sh snapshot info /${VOLUME}/${BUCKET} ${snapshot_name}
    ${snapshot_id} =   Execute    echo '${info}' | jq -r '.snapshotId'
    [Return]           /data/metadata/db.snapshots/checkpointState/om.db-${snapshot_id}.yaml

Snapshot Local YAML Should Show Defragged
    [Arguments]    ${snapshot_name}
    ${yaml} =          Get Snapshot Local YAML Path    ${snapshot_name}
    Execute            test -f '${yaml}'
    ${version} =       Execute    awk '/^[[:space:]]*version:/ {print $2; exit}' '${yaml}'
    ${version} =       Strip String    ${version}
    ${v} =             Convert To Integer    ${version}
    Should Be True     ${v} > 0
    ${needs_defrag} =  Execute    awk '/^[[:space:]]*needsDefrag:/ {print $2; exit}' '${yaml}'
    ${needs_defrag} =  Strip String    ${needs_defrag}
    Should Be Equal    ${needs_defrag}    false

Wait Until Snapshot Local YAML Shows Defragged
    [Arguments]    ${snapshot_name}
    Run Keyword Unless    ${ROCKS_TOOLS_AVAILABLE}    Log
    ...    rocks-tools JNI not loaded in OM; using ${DEFRAG_FALLBACK_SECONDS}s wait instead of YAML defrag poll (build Linux dist with -Drocks_tools_native to exercise reviewer YAML path).
    Run Keyword Unless    ${ROCKS_TOOLS_AVAILABLE}    Sleep    ${DEFRAG_FALLBACK_SECONDS}
    Run Keyword If    ${ROCKS_TOOLS_AVAILABLE}    Wait Until Keyword Succeeds    ${DEFRAG_POLL_TIMEOUT}    ${DEFRAG_POLL_INTERVAL}
    ...    Snapshot Local YAML Should Show Defragged    ${snapshot_name}

Prepare Suite With Bucket And First Snapshot
    Setup volume and bucket
    ${key_one} =            snapshot-setup.Create key           ${VOLUME}       ${BUCKET}       /etc/hosts
    Set Suite Variable      ${KEY_ONE}                          ${key_one}
    ${snapshot_one} =       Create snapshot                     ${VOLUME}       ${BUCKET}
    Set Suite Variable      ${SNAPSHOT_ONE}                     ${snapshot_one}
    Set Suite Variable      ${SNAP_KEY_PATH_ONE}                /${VOLUME}/${BUCKET}/${SNAPSHOT_INDICATOR}/${snapshot_one}/${key_one}
