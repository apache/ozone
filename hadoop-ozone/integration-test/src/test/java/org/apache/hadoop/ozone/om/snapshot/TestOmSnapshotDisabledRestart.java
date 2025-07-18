/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test to verify that if snapshot feature is disabled, OM start up
 * will fail when there are still snapshots remaining.
 */
@Unhealthy("HDDS-8945")
public class TestOmSnapshotDisabledRestart {

  private static MiniOzoneHAClusterImpl cluster = null;
  private static OzoneClient client;
  private static ObjectStore store;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Enable filesystem snapshot feature at the beginning
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test2")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    store = client.getObjectStore();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSnapshotFeatureFlag() throws Exception {
    // Verify that OM start up will indeed fail when there are still snapshots
    // while snapshot feature is disabled.

    String volumeName = "vol-" + RandomStringUtils.secure().nextNumeric(5);
    String bucketName = "buck-" + RandomStringUtils.secure().nextNumeric(5);
    String snapshotName = "snap-" + RandomStringUtils.secure().nextNumeric(5);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    // Create a snapshot
    store.createSnapshot(volumeName, bucketName, snapshotName);

    cluster.getOzoneManagersList().forEach(om -> {
      try {
        cluster.shutdownOzoneManager(om);
        // Disable snapshot feature
        om.getConfiguration().setBoolean(
            OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, false);
        // Restart OM, expect OM start up failure
        RuntimeException rte = assertThrows(RuntimeException.class,
            () -> cluster.restartOzoneManager(om, true));
        assertThat(rte.getMessage()).contains("snapshots remaining");
        // Enable snapshot feature again
        om.getConfiguration().setBoolean(
            OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
        // Should pass this time
        cluster.restartOzoneManager(om, true);
      } catch (Exception e) {
        fail("Failed to restart OM", e);
      }
    });
  }
}
