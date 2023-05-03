/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests snapshot in OM HA setup.
 */
public class TestOzoneManagerHASnapshot extends TestOzoneManagerHA {

  // Test snapshot diff when OM restarts in HA OM env.
  @Test
  public void testSnapshotDiffWhenOmLeaderRestart()
      throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String volumeName = ozoneBucket.getVolumeName();
    String bucketName = ozoneBucket.getName();
    String snapshot1 = "snap-" + RandomStringUtils.randomNumeric(5);
    String snapshot2 = "snap-" + RandomStringUtils.randomNumeric(5);

    createKey(ozoneBucket);
    getObjectStore().createSnapshot(volumeName, bucketName, snapshot1);

    for (int i = 0; i < 100; i++) {
      createKey(ozoneBucket);
    }

    getObjectStore().createSnapshot(volumeName, bucketName, snapshot2);

    SnapshotDiffResponse response =
        getObjectStore().snapshotDiff(volumeName, bucketName,
            snapshot1, snapshot2, null, 0, false);

    assertEquals(IN_PROGRESS, response.getJobStatus());

    String oldLeader = getCluster().getOMLeader().getOMNodeId();

    OzoneManager omLeader = getCluster().getOMLeader();
    getCluster().shutdownOzoneManager(omLeader);
    getCluster().restartOzoneManager(omLeader, true);

    await().atMost(Duration.ofSeconds(120)).
        until(() -> getCluster().getOMLeader() != null);

    String newLeader = getCluster().getOMLeader().getOMNodeId();

    if (Objects.equals(oldLeader, newLeader)) {
      // If old leader becomes leader again. Wait for some time to snapshot diff
      // job finish because OM will load IN_PROGRESS on the startup.
      Thread.sleep(1000L);
      response =
          getObjectStore().snapshotDiff(volumeName, bucketName, snapshot1,
              snapshot2, null, 0, false);
      assertEquals(DONE, response.getJobStatus());
      assertEquals(100, response.getSnapshotDiffReport().getDiffList().size());
    } else {
      // If new leader is different from old leader. SnapDiff request will be
      // new to OM, and job status should be IN_PROGRESS.
      response =
          getObjectStore().snapshotDiff(volumeName, bucketName, snapshot1,
              snapshot2, null, 0, false);
      assertEquals(IN_PROGRESS, response.getJobStatus());
      while (true) {
        response =
            getObjectStore().snapshotDiff(volumeName, bucketName, snapshot1,
                snapshot2, null, 0, false);
        if (DONE == response.getJobStatus()) {
          assertEquals(100,
              response.getSnapshotDiffReport().getDiffList().size());
          break;
        }
        Thread.sleep(response.getWaitTimeInMs());
      }
    }
  }

  @Test
  public void testUniqueSnapshotId() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String volumeName = ozoneBucket.getVolumeName();
    String bucketName = ozoneBucket.getName();

    createKey(ozoneBucket);

    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

    getObjectStore().createSnapshot(volumeName, bucketName, snapshotName);
    List<OzoneManager> ozoneManagers = getCluster().getOzoneManagersList();
    List<String> snapshotIds = new ArrayList<>();

    for (OzoneManager ozoneManager : ozoneManagers) {
      await().atMost(Duration.ofSeconds(120))
          .until(() -> {
            SnapshotInfo snapshotInfo;
            try {
              snapshotInfo = ozoneManager.getMetadataManager()
                  .getSnapshotInfoTable()
                  .get(SnapshotInfo.getTableKey(volumeName,
                      bucketName,
                      snapshotName));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            if (snapshotInfo != null) {
              snapshotIds.add(snapshotInfo.getSnapshotID());
            }
            return snapshotInfo != null;
          });
    }

    assertEquals(1, snapshotIds.stream().distinct().count());
  }
}
