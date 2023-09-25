/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.UUID;

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
  @Timeout(60)
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();

    // Enable filesystem snapshot feature at the beginning
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test2")
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    OzoneManager leaderOzoneManager = cluster.getOMLeader();
    OzoneConfiguration leaderConfig = leaderOzoneManager.getConfiguration();
    cluster.setConf(leaderConfig);
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

    String volumeName = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucketName = "buck-" + RandomStringUtils.randomNumeric(5);
    String snapshotName = "snap-" + RandomStringUtils.randomNumeric(5);

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
        RuntimeException rte = Assertions.assertThrows(RuntimeException.class,
            () -> cluster.restartOzoneManager(om, true));
        Assertions.assertTrue(rte.getMessage().contains("snapshots remaining"));
        // Enable snapshot feature again
        om.getConfiguration().setBoolean(
            OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
        // Should pass this time
        cluster.restartOzoneManager(om, true);
      } catch (Exception e) {
        Assertions.fail("Failed to restart OM", e);
      }
    });
  }
}
