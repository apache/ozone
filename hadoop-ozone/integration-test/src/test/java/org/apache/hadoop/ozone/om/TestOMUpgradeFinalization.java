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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.APPARENT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.upgrade.RatisBasedVersionManager;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for OM upgrade finalization.
 */
class TestOMUpgradeFinalization {

  private static final Logger LOG = LoggerFactory.getLogger(TestOMUpgradeFinalization.class);

  // Force Ratis to take a snapshot and purge logs after only a few
  // transactions, so a lagging OM has to install a snapshot on startup.
  private static final long SNAPSHOT_THRESHOLD = 5;
  private static final int LOG_PURGE_GAP = 5;

  /**
   * A lagging OM that has to install a Ratis snapshot should finalize
   * from that snapshot.
   * When finalization runs on the active OMs while one OM is down, the down OM
   * cannot replay the finalization from the (purged) logs, so it must pick up
   * the finalized version from the checkpoint it downloads from the leader.
   */
  @Test
  void testFinalizationFromSnapshot() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL, "10ms");
    // Force frequent snapshots and log purge so the inactive OM must install a
    // snapshot from the leader (rather than replay logs) when it starts.
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    conf.setLong(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY, SNAPSHOT_THRESHOLD);

    // Use a short client RPC timeout so the first request fails over quickly to
    // a responsive OM instead of blocking on the 15-minute default if it lands on the inactive one.
    OMClientConfig clientConfig = conf.getObject(OMClientConfig.class);
    clientConfig.setRpcTimeOut(TimeUnit.SECONDS.toMillis(5));
    conf.setFromObject(clientConfig);

    LogCapturer versionManagerLogCapture = LogCapturer.captureLogs(RatisBasedVersionManager.class);
    LogCapturer omLogCapture = LogCapturer.captureLogs(OzoneManager.class);

    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      LOG.info("Waiting for cluster to be ready");
      cluster.waitForClusterToBeReady();
      OzoneManager followerOM = cluster.getInactiveOM().next();
      LOG.info("Cluster ready with inactive OM: {}", followerOM);

      try (OzoneClient client = cluster.newClient()) {
        LOG.info("Client created");
        ObjectStore objectStore = client.getObjectStore();
        LOG.info("Object store created");

        // The active OMs start un-finalized: the apparent version is the initial
        // version and no version has been written to the DB yet.
        for (OzoneManager om : cluster.getOzoneManagersList()) {
          if (cluster.isOMActive(om.getOMNodeId())) {
            assertEquals(INITIAL_VERSION, om.getVersionManager().getApparentVersion());
            assertNull(om.getMetadataManager().getMetaTable().get(APPARENT_VERSION_KEY));
          }
        }

        // Finalize the running (active) OMs.
        LOG.info("Finalizing OMs");
        OzoneManagerProtocol omClient = objectStore.getClientProxy().getOzoneManagerClient();
        omClient.forceFinalizeUpgrade();
        OMUpgradeTestUtils.waitForFinalization(omClient);
        LOG.info("Finalized active OMs");

        // Write enough keys to advance the log index well past the purge gap so
        // the leader's logs covering finalization are purged. This forces the
        // follower to install a snapshot instead of replaying the logs.
        OzoneManager leaderOM = cluster.getOMLeader();
        assertNotNull(leaderOM);
        OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();
        OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster.newClient());
        long targetLogIndex = leaderRatisServer.getLastAppliedTermIndex().getIndex() + 100;
        LOG.info("Writing keys to advance log index");
        writeKeysToIncreaseLogIndex(leaderRatisServer, targetLogIndex, bucket);

        // Start the inactive OM. It downloads the leader's checkpoint (which
        // already contains the finalized apparent version) and finalizes from it.
        LOG.info("Starting inactive OM");
        cluster.startInactiveOM(followerOM.getOMNodeId());

        // Wait for the follower to finish installing the snapshot and catch up
        // to the leader before reading its DB and shutting down, so shutdown
        // does not race with the tail of the snapshot install.
        LOG.info("Inactive OM started, waiting to catch up with leader");
        leaderOM = cluster.getOMLeader();
        assertNotNull(leaderOM);
        long leaderIndex = leaderOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
        waitFor(() -> {
          boolean running = followerOM.isRunning();
          // Make sure the snapshot install finishes completely so it does not block cluster shutdown on test cleanup.
          boolean snapshotFinished = omLogCapture.getOutput().contains("Install Checkpoint is finished");
          long followerIndex = followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
          LOG.info("Waiting for follower to catch up with leader. Running? {} Snapshot finished? {}" +
                  " Follower index = {} leader index = {}",
              running, snapshotFinished, followerIndex, leaderIndex);
          return running && snapshotFinished && followerIndex >= leaderIndex;
        }, 1000, 60000);
        LOG.info("Inactive OM has caught up with leader, checking finalization state");
        omLogCapture.stopCapturing();

        // The follower finalizes to the software version, picked up from the installed snapshot.
        assertEquals(OzoneManagerVersion.SOFTWARE_VERSION, followerOM.getVersionManager().getApparentVersion());

        String dbVersion = followerOM.getMetadataManager().getMetaTable().get(APPARENT_VERSION_KEY);
        assertNotNull(dbVersion);
        assertEquals(OzoneManagerVersion.SOFTWARE_VERSION.serialize(), Integer.parseInt(dbVersion));

        // Confirm finalization happened via snapshot install, not log replay.
        assertThat(versionManagerLogCapture.getOutput()).contains("New snapshot received with higher apparent version");
        versionManagerLogCapture.stopCapturing();
      }
    }
  }

  private static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf)
      throws IOException {
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, INITIAL_VERSION.serialize());
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(UUID.randomUUID().toString())
        .setNumOfOzoneManagers(3)
        .setNumOfActiveOMs(2)
        .setNumDatanodes(1);
    return builder.build();
  }

  private static void writeKeysToIncreaseLogIndex(OzoneManagerRatisServer omRatisServer,
                                                  long targetLogIndex, OzoneBucket bucket) throws IOException {
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      createKey(bucket);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
  }
}
