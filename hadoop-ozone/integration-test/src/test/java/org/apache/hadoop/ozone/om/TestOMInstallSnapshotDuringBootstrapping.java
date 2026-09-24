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

import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithStoppedNodes.createKey;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.RDBSnapshotProvider;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for OM bootstrap install snapshot while {@code BOOTSTRAPPING}.
 */
public class TestOMInstallSnapshotDuringBootstrapping {

  private static final String OM_SERVICE_ID = "om-service-bootstrap";
  private static final int LOG_PURGE_GAP = 5;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final long TARGET_LOG_INDEX = 200;
  private static final int INSTALL_START_DEADLINE_MS = 30_000;
  private static final int COMPLETION_DEADLINE_MS = 60_000;
  private static final BucketLayout TEST_BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;

  private MiniOzoneHAClusterImpl cluster;
  private OzoneClient client;
  private OzoneBucket ozoneBucket;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 5);
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY,
        16, StorageUnit.KB);

    OzoneManagerRatisServerConfig omRatisConf =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(2)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();

    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
    ObjectStore objectStore = client.getObjectStore();
    String volumeName = uniqueObjectName("volume");
    String bucketName = uniqueObjectName("bucket");
    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = volume.getBucket(bucketName);
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Checkpoint install must proceed during {@code BOOTSTRAPPING} with the default
   * v2 checkpoint API and complete successfully.
   */
  @Test
  public void testInstallSnapshotDuringBootstrapping() throws Exception {
    OzoneManager leader = cluster.getOMLeader();
    writeKeysToIncreaseLogIndex(leader.getOmRatisServer(), TARGET_LOG_INDEX);
    assertThat(leader.getRatisSnapshotIndex())
        .as("leader should have purged early logs")
        .isGreaterThan((long) LOG_PURGE_GAP);

    LogCapturer omLog = LogCapturer.captureLogs(OzoneManager.class);
    LogCapturer stateMachineLog =
        LogCapturer.captureLogs(OzoneManagerStateMachine.class);
    LogCapturer snapshotProviderLog =
        LogCapturer.captureLogs(RDBSnapshotProvider.class);
    String newNodeId = "omNode-bootstrap-ratis-snapshots";
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> bootstrapFuture = executor.submit(() -> {
      try {
        cluster.bootstrapOzoneManager(newNodeId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    try {
      waitForCheckpointInstallToStart(omLog, snapshotProviderLog);
      bootstrapFuture.get(COMPLETION_DEADLINE_MS, TimeUnit.MILLISECONDS);
      assertBootstrapOmJoinedRatisGroup(newNodeId);
    } finally {
      bootstrapFuture.cancel(true);
      omLog.stopCapturing();
      stateMachineLog.stopCapturing();
      snapshotProviderLog.stopCapturing();
      executor.shutdownNow();
    }

    assertThat(stateMachineLog.getOutput())
        .as("Ratis should notify the bootstrapping OM to install a checkpoint")
        .contains("Received install snapshot notification from OM leader");
    assertThat(omLog.getOutput())
        .as("checkpoint install must not be aborted during BOOTSTRAPPING")
        .doesNotContain("Abort install snapshot from Leader");
    assertThat(omLog.getOutput())
        .as("checkpoint installation should finish")
        .contains("Install Checkpoint is finished");
    assertThat(snapshotProviderLog.getOutput())
        .as("checkpoint download should start after install is accepted")
        .contains("Prepare to download the snapshot from leader OM");
    assertThat(snapshotProviderLog.getOutput())
        .as("checkpoint tarball should be assembled on the bootstrapping OM")
        .contains("DB snapshot transfer is complete.");
  }

  private void writeKeysToIncreaseLogIndex(OzoneManagerRatisServer omRatisServer,
      long targetLogIndex) throws Exception {
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      createKey(ozoneBucket);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
  }

  private void assertBootstrapOmJoinedRatisGroup(String newNodeId) {
    OzoneManager newOm = cluster.getOzoneManager(newNodeId);
    assertNotNull(newOm, "Bootstrapped OM should be registered on the cluster");
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      assertTrue(om.doesPeerExist(newNodeId),
          "New OM node " + newNodeId + " not present in peer list of OM " + om.getOMNodeId());
      assertTrue(om.getOmRatisServer().doesPeerExist(newNodeId),
          "New OM node " + newNodeId + " not present in Ratis peer list of OM "
              + om.getOMNodeId());
    }
  }

  private void waitForCheckpointInstallToStart(LogCapturer omLog,
      LogCapturer snapshotProviderLog) throws InterruptedException, TimeoutException {
    try {
      GenericTestUtils.waitFor(() -> {
        if (omLog.getOutput().contains("Abort install snapshot from Leader")) {
          fail("Checkpoint install was aborted during BOOTSTRAPPING.");
        }
        return snapshotProviderLog.getOutput()
            .contains("Prepare to download the snapshot from leader OM");
      }, 200, INSTALL_START_DEADLINE_MS);
    } catch (TimeoutException e) {
      fail("Checkpoint download did not start within " + INSTALL_START_DEADLINE_MS
          + "ms. OzoneManager log: " + omLog.getOutput()
          + ", RDBSnapshotProvider log: " + snapshotProviderLog.getOutput());
    }
  }
}
