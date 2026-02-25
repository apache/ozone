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

import static org.apache.hadoop.ozone.om.TestOMRatisSnapshots.checkSnapshot;
import static org.apache.hadoop.ozone.om.TestOMRatisSnapshots.createOzoneSnapshot;
import static org.apache.hadoop.ozone.om.TestOMRatisSnapshots.writeKeys;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.conf.OMClientConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that bootstrapping a follower works for v1 checkpoint format.
 * A stopped follower is restarted after the leader has advanced beyond
 * the purge gap, forcing InstallSnapshot/bootstrap.
 */
public class TestOMBootstrap {

  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final int NUM_OF_OMS = 3;
  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final BucketLayout TEST_BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;

  private MiniOzoneHAClusterImpl cluster;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;
  private OzoneClient client;

  /**
   * Override in subclasses to use v2 checkpoint format.
   */
  protected boolean useV2CheckpointFormat() {
    return false;
  }

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_DB_CHECKPOINT_USE_V2_KEY, useV2CheckpointFormat());
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, 5);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    conf.setLong(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY, SNAPSHOT_THRESHOLD);

    OzoneManagerRatisServerConfig omRatisConf = conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);

    OMClientConfig clientConfig = conf.getObject(OMClientConfig.class);
    clientConfig.setRpcTimeOut(TimeUnit.SECONDS.toMillis(5));
    conf.setFromObject(clientConfig);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfActiveOMs(NUM_OF_OMS)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(OM_SERVICE_ID, conf);
    objectStore = client.getObjectStore();

    volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.secure().nextNumeric(5))
        .setAdmin("admin" + RandomStringUtils.secure().nextNumeric(5))
        .build();
    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);
    retVolumeinfo.createBucket(bucketName,
        BucketArgs.newBuilder().setBucketLayout(TEST_BUCKET_LAYOUT).build());
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBootstrapFollower() throws Exception {
    final String leaderOMNodeId = OmTestUtil
        .getCurrentOmProxyNodeId(objectStore);
    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    cluster.stopOzoneManager(followerNodeId);
    GenericTestUtils.waitFor(() -> !followerOM.isRunning() &&
            !followerOM.isOmRpcServerRunning(),
        100, 15_000);

    List<String> keys = writeKeys(ozoneBucket, 25);
    SnapshotInfo snapshotInfo1 = createOzoneSnapshot(objectStore, volumeName,
        bucketName, leaderOM, "snap1");
    List<String> moreKeys = writeKeys(ozoneBucket, 5);
    SnapshotInfo snapshotInfo2 = createOzoneSnapshot(objectStore, volumeName,
        bucketName, leaderOM, "snap2");

    LogCapturer logCapture = LogCapturer.captureLogs(OzoneManager.class);
    cluster.restartOzoneManager(followerOM, true);

    TransactionInfo transactionInfo = TransactionInfo.readTransactionInfo(
        leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex = TermIndex.valueOf(transactionInfo.getTerm(),
        transactionInfo.getTransactionIndex());
    long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();

    GenericTestUtils.waitFor(() -> {
      long index = followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
      return index >= leaderOMSnapshotIndex - 1;
    }, 100, 30_000);

    assertLogCapture(logCapture, "Reloaded OM state");
    assertLogCapture(logCapture, "Install Checkpoint is finished");

    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
    for (String key : moreKeys) {
      assertNotNull(followerOMMetaMngr.getKeyTable(TEST_BUCKET_LAYOUT)
          .get(followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }

    GenericTestUtils.waitFor(() -> followerOM.isOmRpcServerRunning(),
        100, 30_000);

    checkSnapshot(volumeName, bucketName, leaderOM, followerOM,
        "snap1", keys, snapshotInfo1);
    checkSnapshot(volumeName, bucketName, leaderOM, followerOM,
        "snap2", moreKeys, snapshotInfo2);
  }

  private void assertLogCapture(LogCapturer logCapture, String msg)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> logCapture.getOutput().contains(msg), 100, 30_000);
  }
}
