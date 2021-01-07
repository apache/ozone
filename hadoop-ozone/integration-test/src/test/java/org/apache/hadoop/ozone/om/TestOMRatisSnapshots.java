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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.ratis.OMTransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.util.ExitManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;

import static org.apache.hadoop.ozone.om.TestOzoneManagerHAWithData.createKey;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Tests the Ratis snaphsots feature in OM.
 */
public class TestOMRatisSnapshots {

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private int numOfOMs = 3;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;

  private static final long SNAPSHOT_THRESHOLD = 50;
  private static final int LOG_PURGE_GAP = 50;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(500_000);

  /**
   * Create a MiniOzoneCluster for testing. The cluster initially has one
   * inactive OM. So at the start of the cluster, there will be 2 active and 1
   * inactive OM.
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(
        OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY,
        SNAPSHOT_THRESHOLD);
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId("om-service-test1")
        .setNumOfOzoneManagers(numOfOMs)
        .setNumOfActiveOMs(2)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();

    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = VolumeArgs.newBuilder()
        .setOwner("user" + RandomStringUtils.randomNumeric(5))
        .setAdmin("admin" + RandomStringUtils.randomNumeric(5))
        .build();

    objectStore.createVolume(volumeName, createVolumeArgs);
    OzoneVolume retVolumeinfo = objectStore.getVolume(volumeName);

    retVolumeinfo.createBucket(bucketName);
    ozoneBucket = retVolumeinfo.getBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInstallSnapshot() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getOMNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getOMNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

    // Do some transactions so that the log index increases
    List<String> keys = writeKeysToIncreaseLogIndex(leaderRatisServer, 200);

    // Get the latest db checkpoint from the leader OM.
    OMTransactionInfo omTransactionInfo =
        OMTransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
    TermIndex leaderOMTermIndex =
        TermIndex.valueOf(omTransactionInfo.getTerm(),
            omTransactionInfo.getTransactionIndex());
    long leaderOMSnaphsotIndex = leaderOMTermIndex.getIndex();
    long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

    DBCheckpoint leaderDbCheckpoint =
        leaderOM.getMetadataManager().getStore().getCheckpoint(false);

    // Start the inactive OM
    cluster.startInactiveOM(followerNodeId);

    // The recently started OM should be lagging behind the leader OM.
    long followerOMLastAppliedIndex =
        followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
    assertTrue(
        followerOMLastAppliedIndex < leaderOMSnaphsotIndex);

    // Install leader OM's db checkpoint on the lagging OM.
    followerOM.installCheckpoint(leaderOMNodeId, leaderDbCheckpoint);

    // After the new checkpoint is installed, the follower OM
    // lastAppliedIndex must >= the snapshot index of the checkpoint. It
    // could be great than snapshot index if there is any conf entry from ratis.
    followerOMLastAppliedIndex = followerOM.getOmRatisServer()
        .getLastAppliedTermIndex().getIndex();
    assertTrue(followerOMLastAppliedIndex >= leaderOMSnaphsotIndex);
    assertTrue(followerOM.getOmRatisServer().getLastAppliedTermIndex()
        .getTerm() >= leaderOMSnapshotTermIndex);

    // Verify that the follower OM's DB contains the transactions which were
    // made while it was inactive.
    OMMetadataManager followerOMMetaMngr = followerOM.getMetadataManager();
    Assert.assertNotNull(followerOMMetaMngr.getVolumeTable().get(
        followerOMMetaMngr.getVolumeKey(volumeName)));
    Assert.assertNotNull(followerOMMetaMngr.getBucketTable().get(
        followerOMMetaMngr.getBucketKey(volumeName, bucketName)));
    for (String key : keys) {
      Assert.assertNotNull(followerOMMetaMngr.getKeyTable().get(
          followerOMMetaMngr.getOzoneKey(volumeName, bucketName, key)));
    }
  }

  @Test
  public void testInstallOldCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

    // Find the inactive OM and start it
    String followerNodeId = leaderOM.getPeerNodes().get(0).getOMNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getOMNodeId();
    }
    cluster.startInactiveOM(followerNodeId);

    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    OzoneManagerRatisServer followerRatisServer = followerOM.getOmRatisServer();

    // Do some transactions so that the log index increases on follower OM
    writeKeysToIncreaseLogIndex(followerRatisServer, 100);

    TermIndex leaderCheckpointTermIndex = leaderOM.getOmRatisServer()
        .getLastAppliedTermIndex();
    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);

    // Do some more transactions to increase the log index further on
    // follower OM such that it is more than the checkpoint index taken on
    // leader OM.
    writeKeysToIncreaseLogIndex(followerOM.getOmRatisServer(),
        leaderCheckpointTermIndex.getIndex() + 100);

    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.INFO);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);

    // Install the old checkpoint on the follower OM. This should fail as the
    // followerOM is already ahead of that transactionLogIndex and the OM
    // state should be reloaded.
    TermIndex followerTermIndex = followerRatisServer.getLastAppliedTermIndex();
    TermIndex newTermIndex = followerOM.installCheckpoint(
        leaderOMNodeId, leaderDbCheckpoint);

    String errorMsg = "Cannot proceed with InstallSnapshot as OM is at " +
        "TermIndex " + followerTermIndex + " and checkpoint has lower " +
        "TermIndex";
    Assert.assertTrue(logCapture.getOutput().contains(errorMsg));
    Assert.assertNull("OM installed checkpoint even though checkpoint " +
        "logIndex is less than it's lastAppliedIndex", newTermIndex);
    Assert.assertEquals(followerTermIndex,
        followerRatisServer.getLastAppliedTermIndex());
  }

  @Test
  public void testInstallCorruptedCheckpointFailure() throws Exception {
    // Get the leader OM
    String leaderOMNodeId = OmFailoverProxyUtil
        .getFailoverProxyProvider(objectStore.getClientProxy())
        .getCurrentProxyOMNodeId();

    OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);
    OzoneManagerRatisServer leaderRatisServer = leaderOM.getOmRatisServer();

    // Find the inactive OM
    String followerNodeId = leaderOM.getPeerNodes().get(0).getOMNodeId();
    if (cluster.isOMActive(followerNodeId)) {
      followerNodeId = leaderOM.getPeerNodes().get(1).getOMNodeId();
    }
    OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);
    OzoneManagerRatisServer followerRatisServer = followerOM.getOmRatisServer();

    // Do some transactions so that the log index increases
    writeKeysToIncreaseLogIndex(leaderRatisServer, 100);

    DBCheckpoint leaderDbCheckpoint = leaderOM.getMetadataManager().getStore()
        .getCheckpoint(false);
    Path leaderCheckpointLocation = leaderDbCheckpoint.getCheckpointLocation();
    OMTransactionInfo leaderCheckpointTrxnInfo = OzoneManagerRatisUtils
        .getTrxnInfoFromCheckpoint(conf, leaderCheckpointLocation);

    // Corrupt the leader checkpoint and install that on the OM. The
    // operation should fail and OM should shutdown.
    boolean delete = true;
    for (File file : leaderCheckpointLocation.toFile()
        .listFiles()) {
      if (file.getName().contains(".sst")) {
        if (delete) {
          file.delete();
          delete = false;
        } else {
          delete = true;
        }
      }
    }

    GenericTestUtils.setLogLevel(OzoneManager.LOG, Level.ERROR);
    GenericTestUtils.LogCapturer logCapture =
        GenericTestUtils.LogCapturer.captureLogs(OzoneManager.LOG);
    followerOM.setExitManagerForTesting(new DummyExitManager());

    followerOM.installCheckpoint(leaderOMNodeId, leaderCheckpointLocation,
        leaderCheckpointTrxnInfo);

    Assert.assertTrue(logCapture.getOutput().contains("System Exit: " +
        "Failed to reload OM state and instantiate services."));
  }

  private List<String> writeKeysToIncreaseLogIndex(
      OzoneManagerRatisServer omRatisServer, long targetLogIndex)
      throws IOException, InterruptedException {
    List<String> keys = new ArrayList<>();
    long logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    while (logIndex < targetLogIndex) {
      keys.add(createKey(ozoneBucket));
      Thread.sleep(100);
      logIndex = omRatisServer.getLastAppliedTermIndex().getIndex();
    }
    return keys;
  }

  private class DummyExitManager extends ExitManager {
    @Override
    public void exitSystem(int status, String message, Throwable throwable,
        Logger log) {
      log.error("System Exit: " + message, throwable);
    }
  }
}
