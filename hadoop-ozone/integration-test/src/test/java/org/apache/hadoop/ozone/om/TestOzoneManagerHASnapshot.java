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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests snapshot in OM HA setup.
 */
@Timeout(300)
@Disabled("HDDS-8645")
public class TestOzoneManagerHASnapshot extends TestOzoneManagerHA {

  /**
   * Test snapshotNames are unique among OM nodes when snapshotName is not
   * passed or empty.
   */
  @Test
  public void testSnapshotNameConsistency() throws Exception {
    OzoneBucket ozoneBucket = setupBucket();
    String volumeName = ozoneBucket.getVolumeName();
    String bucketName = ozoneBucket.getName();

    createKey(ozoneBucket);

    getObjectStore().createSnapshot(volumeName, bucketName, "");
    List<OzoneManager> ozoneManagers = getCluster().getOzoneManagersList();
    List<String> snapshotNames = new ArrayList<>();

    for (OzoneManager ozoneManager : ozoneManagers) {
      await().atMost(Duration.ofSeconds(120))
          .until(() -> {
            String snapshotPrefix = OM_KEY_PREFIX + volumeName +
                OM_KEY_PREFIX + bucketName;
            SnapshotInfo snapshotInfo = null;
            try (TableIterator<String, ?
                extends Table.KeyValue<String, SnapshotInfo>>
                     iterator = ozoneManager.getMetadataManager()
                .getSnapshotInfoTable().iterator(snapshotPrefix)) {
              while (iterator.hasNext()) {
                snapshotInfo = iterator.next().getValue();
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            if (snapshotInfo != null) {
              snapshotNames.add(snapshotInfo.getName());
            }
            return snapshotInfo != null;
          });
    }

    assertEquals(1, snapshotNames.stream().distinct().count());
    assertNotNull(snapshotNames.get(0));
    assertTrue(snapshotNames.get(0).startsWith("s"));
  }

  @Test
  public void testSnapshotChainManagerRestore() throws Exception {
    List<OzoneBucket> ozoneBuckets = new ArrayList<>();
    List<String> volumeNames = new ArrayList<>();
    List<String> bucketNames = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      OzoneBucket ozoneBucket = setupBucket();
      ozoneBuckets.add(ozoneBucket);
      volumeNames.add(ozoneBucket.getVolumeName());
      bucketNames.add(ozoneBucket.getName());
    }

    for (int i = 0; i < 100; i++) {
      int index = i % 10;
      createKey(ozoneBuckets.get(index));
      String snapshot1 = "snapshot-" + RandomStringUtils.randomNumeric(5);
      getObjectStore().createSnapshot(volumeNames.get(index),
          bucketNames.get(index), snapshot1);
    }

    // Restart leader OM
    OzoneManager omLeader = getCluster().getOMLeader();
    getCluster().shutdownOzoneManager(omLeader);
    getCluster().restartOzoneManager(omLeader, true);

    await().atMost(Duration.ofSeconds(180))
        .until(() -> getCluster().getOMLeader() != null);
    assertNotNull(getCluster().getOMLeader());
  }
}
