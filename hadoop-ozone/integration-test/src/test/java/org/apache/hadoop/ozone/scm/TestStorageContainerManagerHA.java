/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMHAMetrics;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;

/**
 * Base class for SCM HA tests.
 */
@Timeout(300)
public class TestStorageContainerManagerHA {

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private static int numOfOMs = 3;
  private String scmServiceId;
  private static int numOfSCMs = 3;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        "5s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, "1");
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfOzoneManagers(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    DefaultConfigManager.clearDefaultConfigs();
  }

  @Test
  public void testAllSCMAreRunning() throws Exception {
    int count = 0;
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    Assertions.assertEquals(numOfSCMs, scms.size());
    int peerSize = cluster.getStorageContainerManager().getScmHAManager()
        .getRatisServer().getDivision().getGroup().getPeers().size();
    StorageContainerManager leaderScm = null;
    for (StorageContainerManager scm : scms) {
      if (scm.checkLeader()) {
        count++;
        leaderScm = scm;
      }
      Assertions.assertEquals(peerSize, numOfSCMs);
    }
    Assertions.assertEquals(1, count);
    count = 0;
    List<OzoneManager> oms = cluster.getOzoneManagersList();
    Assertions.assertEquals(numOfOMs, oms.size());
    for (OzoneManager om : oms) {
      if (om.isLeaderReady()) {
        count++;
      }
    }
    Assertions.assertEquals(1, count);
    
    // verify timer based transaction buffer flush is working
    SnapshotInfo latestSnapshot = leaderScm.getScmHAManager()
        .asSCMHADBTransactionBuffer().getLatestSnapshot();
    testPutKey();
    final StorageContainerManager leaderScmTmp = leaderScm;
    GenericTestUtils.waitFor(() -> {
      if (leaderScmTmp.getScmHAManager().asSCMHADBTransactionBuffer()
          .getLatestSnapshot() != null) {
        if (leaderScmTmp.getScmHAManager().asSCMHADBTransactionBuffer()
            .getLatestSnapshot().getIndex() > latestSnapshot.getIndex()) {
          return true;
        }
      }
      return false;
    }, 2000, 30000);
  }

  @Test
  public void testPutKey() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = Instant.now();
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      String value = "sample value";
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);

      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket
          .createKey(keyName, value.getBytes(UTF_8).length, RATIS, ONE,
              new HashMap<>());
      out.write(value.getBytes(UTF_8));
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assertions.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes(UTF_8).length];
      is.read(fileContent);
      Assertions.assertEquals(value, new String(fileContent, UTF_8));
      Assertions.assertFalse(key.getCreationTime().isBefore(testStartTime));
      Assertions.assertFalse(key.getModificationTime().isBefore(testStartTime));
      is.close();
      final OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .setReplicationConfig(RatisReplicationConfig.getInstance(
              HddsProtos.ReplicationFactor.ONE))
          .setKeyName(keyName)
          .build();
      final OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);
      final List<OmKeyLocationInfo> keyLocationInfos =
          keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
      long index = -1;
      for (StorageContainerManager scm : cluster
          .getStorageContainerManagers()) {
        if (scm.checkLeader()) {
          index = getLastAppliedIndex(scm);
        }
      }
      Assertions.assertNotEquals(-1, index);
      long finalIndex = index;
      // Ensure all follower scms have caught up with the leader
      GenericTestUtils.waitFor(() -> areAllScmInSync(finalIndex), 100, 10000);
      final long containerID = keyLocationInfos.get(0).getContainerID();
      for (int k = 0; k < numOfSCMs; k++) {
        StorageContainerManager scm =
            cluster.getStorageContainerManagers().get(k);
        // flush to DB on each SCM
        ((SCMRatisServerImpl) scm.getScmHAManager().getRatisServer())
            .getStateMachine().takeSnapshot();
        Assertions.assertTrue(scm.getContainerManager()
            .containerExist(ContainerID.valueOf(containerID)));
        Assertions.assertNotNull(scm.getScmMetadataStore().getContainerTable()
            .get(ContainerID.valueOf(containerID)));
      }
    }
  }

  private long getLastAppliedIndex(StorageContainerManager scm) {
    return scm.getScmHAManager().getRatisServer().getDivision().getInfo()
        .getLastAppliedIndex();
  }

  private boolean areAllScmInSync(long leaderIndex) {
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    boolean sync = false;
    for (StorageContainerManager scm : scms) {
      sync = getLastAppliedIndex(scm) == leaderIndex;
    }
    return sync;
  }

  @Test
  public void testPrimordialSCM() throws Exception {
    StorageContainerManager scm1 = cluster.getStorageContainerManagers().get(0);
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf1 = scm1.getConfiguration();
    OzoneConfiguration conf2 = scm2.getConfiguration();
    conf1.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    conf2.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    Assertions.assertTrue(StorageContainerManager.scmBootstrap(conf1));
    scm1.getScmHAManager().stop();
    Assertions.assertTrue(
        StorageContainerManager.scmInit(conf1, scm1.getClusterId()));
    Assertions.assertTrue(StorageContainerManager.scmBootstrap(conf2));
    Assertions.assertTrue(
        StorageContainerManager.scmInit(conf2, scm2.getClusterId()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testHAConfig(boolean isRatisEnabled) throws Exception {
    StorageContainerManager scm0 = cluster.getStorageContainerManager(0);
    scm0.stop();
    boolean isDeleted = scm0.getScmStorageConfig().getVersionFile().delete();
    Assertions.assertTrue(isDeleted);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, isRatisEnabled);
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    scmStorageConfig.getCurrentDir().delete();
    scmStorageConfig.setSCMHAFlag(isRatisEnabled);
    DefaultConfigManager.clearDefaultConfigs();
    scmStorageConfig.initialize();
    StorageContainerManager.scmInit(conf, clusterId);
    Assertions.assertEquals(DefaultConfigManager.getValue(
        ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, !isRatisEnabled),
        isRatisEnabled);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInvalidHAConfig(boolean isRatisEnabled) throws Exception {
    StorageContainerManager scm0 = cluster.getStorageContainerManager(0);
    scm0.stop();
    boolean isDeleted = scm0.getScmStorageConfig().getVersionFile().delete();
    Assertions.assertTrue(isDeleted);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, isRatisEnabled);
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    scmStorageConfig.getCurrentDir().delete();
    scmStorageConfig.setSCMHAFlag(!isRatisEnabled);
    DefaultConfigManager.clearDefaultConfigs();
    scmStorageConfig.initialize();
    Assertions.assertThrows(ConfigurationException.class,
        () -> StorageContainerManager.scmInit(conf, clusterId));
  }

  @Test
  public void testBootStrapSCM() throws Exception {
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf2 = scm2.getConfiguration();
    boolean isDeleted = scm2.getScmStorageConfig().getVersionFile().delete();
    Assertions.assertTrue(isDeleted);
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf2);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    scmStorageConfig.getCurrentDir().delete();
    scmStorageConfig.setSCMHAFlag(true);
    scmStorageConfig.initialize();
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        false);
    Assertions.assertFalse(StorageContainerManager.scmBootstrap(conf2));
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        true);
    Assertions.assertTrue(StorageContainerManager.scmBootstrap(conf2));
  }

  @Test
  public void testGetRatisRolesDetail() {
    Set<String> resultSet = new HashSet<>();
    for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
      resultSet.addAll(scm.getScmHAManager().getRatisServer().getRatisRoles());
    }
    System.out.println(resultSet);
    Assertions.assertEquals(3, resultSet.size());
    Assertions.assertEquals(1,
        resultSet.stream().filter(x -> x.contains("LEADER")).count());
  }

  @Test
  public void testSCMHAMetrics() throws InterruptedException, TimeoutException {
    waitForLeaderToBeReady();

    StorageContainerManager leaderSCM = cluster.getActiveSCM();
    String leaderSCMId = leaderSCM.getScmId();
    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();

    checkSCMHAMetricsForAllSCMs(scms, leaderSCMId);
  }

  private void checkSCMHAMetricsForAllSCMs(List<StorageContainerManager> scms,
      String leaderSCMId) {
    for (StorageContainerManager scm : scms) {
      String nodeId = scm.getScmId();

      SCMHAMetrics scmHAMetrics = scm.getScmHAMetrics();
      // If current SCM is leader, state should be 1
      int expectedState = nodeId.equals(leaderSCMId) ? 1 : 0;

      Assertions.assertEquals(expectedState,
          scmHAMetrics.getSCMHAMetricsInfoLeaderState());
      Assertions.assertEquals(nodeId, scmHAMetrics.getSCMHAMetricsInfoNodeId());
    }
  }

  /**
   * Some tests are stopping or restarting SCMs.
   * There are test cases where we might need to
   * wait for a leader to be elected and ready.
   */
  private void waitForLeaderToBeReady()
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        return cluster.getActiveSCM().checkLeader();
      } catch (Exception e) {
        return false;
      }
    }, 1000, (int) ScmConfigKeys
        .OZONE_SCM_HA_RATIS_LEADER_READY_WAIT_TIMEOUT_DEFAULT);
  }
}
