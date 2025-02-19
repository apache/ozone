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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for SCM HA.
 */
@Timeout(300)
public class TestStorageContainerManagerHA {

  private static final Logger LOG = LoggerFactory.getLogger(TestStorageContainerManagerHA.class);

  private MiniOzoneHAClusterImpl cluster;
  private OzoneConfiguration conf;
  private static final int OM_COUNT = 3;
  private static final int SCM_COUNT = 3;

  public void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        "5s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, "1");
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setSCMServiceId("scm-service-test1")
        .setNumOfStorageContainerManagers(SCM_COUNT)
        .setNumOfOzoneManagers(OM_COUNT)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void testAllSCMAreRunning() throws Exception {
    init();
    int count = 0;
    List<StorageContainerManager> scms = cluster.getStorageContainerManagers();
    assertEquals(SCM_COUNT, scms.size());
    int peerSize = cluster.getStorageContainerManager().getScmHAManager()
        .getRatisServer().getDivision().getGroup().getPeers().size();
    StorageContainerManager leaderScm = null;
    for (StorageContainerManager scm : scms) {
      if (scm.checkLeader()) {
        count++;
        leaderScm = scm;
      }
      assertNotNull(scm.getScmHAManager().getRatisServer().getLeaderId());
      assertEquals(peerSize, SCM_COUNT);
    }
    assertEquals(1, count);
    assertNotNull(leaderScm);

    assertRatisRoles();

    String leaderSCMId = leaderScm.getScmId();
    checkSCMHAMetricsForAllSCMs(scms, leaderSCMId);

    count = 0;
    List<OzoneManager> oms = cluster.getOzoneManagersList();
    assertEquals(OM_COUNT, oms.size());
    for (OzoneManager om : oms) {
      if (om.isLeaderReady()) {
        count++;
      }
    }
    assertEquals(1, count);
    
    // verify timer based transaction buffer flush is working
    SnapshotInfo latestSnapshot = leaderScm.getScmHAManager()
        .asSCMHADBTransactionBuffer()
        .getLatestSnapshot();
    doPutKey();
    final StorageContainerManager leaderScmTmp = leaderScm;
    GenericTestUtils.waitFor(() -> {
      SnapshotInfo newSnapshot = leaderScmTmp.getScmHAManager()
          .asSCMHADBTransactionBuffer()
          .getLatestSnapshot();
      return newSnapshot != null && newSnapshot.getIndex() > latestSnapshot.getIndex();
    }, 2000, 30000);
  }

  private void doPutKey() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    Instant testStartTime = getTestStartTime();
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      String value = "sample value";
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);

      String keyName = UUID.randomUUID().toString();

      byte[] bytes = value.getBytes(UTF_8);
      try (OutputStream out = bucket.createKey(keyName, bytes.length, RATIS, ONE, new HashMap<>())) {
        out.write(bytes);
      }

      OzoneKey key = bucket.getKey(keyName);
      assertEquals(keyName, key.getName());

      try (OzoneInputStream is = bucket.readKey(keyName)) {
        byte[] fileContent = new byte[bytes.length];
        assertEquals(fileContent.length, is.read(fileContent));
        assertEquals(value, new String(fileContent, UTF_8));
        assertFalse(key.getCreationTime().isBefore(testStartTime));
        assertFalse(key.getModificationTime().isBefore(testStartTime));
      }

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
      assertNotEquals(-1, index);
      long finalIndex = index;
      // Ensure all follower scms have caught up with the leader
      GenericTestUtils.waitFor(() -> areAllScmInSync(finalIndex), 100, 10000);
      final long containerID = keyLocationInfos.get(0).getContainerID();
      for (int k = 0; k < SCM_COUNT; k++) {
        StorageContainerManager scm =
            cluster.getStorageContainerManagers().get(k);
        // flush to DB on each SCM
        ((SCMRatisServerImpl) scm.getScmHAManager().getRatisServer())
            .getStateMachine().takeSnapshot();
        assertTrue(scm.getContainerManager()
            .containerExist(ContainerID.valueOf(containerID)));
        assertNotNull(scm.getScmMetadataStore().getContainerTable()
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
    init();
    StorageContainerManager scm1 = cluster.getStorageContainerManagers().get(0);
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf1 = scm1.getConfiguration();
    OzoneConfiguration conf2 = scm2.getConfiguration();
    conf1.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    conf2.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    assertTrue(StorageContainerManager.scmBootstrap(conf1));
    scm1.getScmHAManager().stop();
    assertTrue(
        StorageContainerManager.scmInit(conf1, scm1.getClusterId()));
    assertTrue(StorageContainerManager.scmBootstrap(conf2));
    assertTrue(StorageContainerManager.scmInit(conf2, scm2.getClusterId()));
  }

  @Test
  public void testBootStrapSCM() throws Exception {
    init();
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf2 = scm2.getConfiguration();
    boolean isDeleted = scm2.getScmStorageConfig().getVersionFile().delete();
    assertTrue(isDeleted);
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf2);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    scmStorageConfig.getCurrentDir().delete();
    scmStorageConfig.setSCMHAFlag(true);
    scmStorageConfig.initialize();
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        false);
    assertFalse(StorageContainerManager.scmBootstrap(conf2));
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        true);
    assertTrue(StorageContainerManager.scmBootstrap(conf2));
  }

  private void assertRatisRoles() {
    Set<String> resultSet = new HashSet<>();
    for (StorageContainerManager scm: cluster.getStorageContainerManagers()) {
      resultSet.addAll(scm.getScmHAManager().getRatisServer().getRatisRoles());
    }
    System.out.println(resultSet);
    assertEquals(3, resultSet.size());
    assertEquals(1,
        resultSet.stream().filter(x -> x.contains("LEADER")).count());
  }

  private void checkSCMHAMetricsForAllSCMs(List<StorageContainerManager> scms,
      String leaderSCMId) {
    for (StorageContainerManager scm : scms) {
      String nodeId = scm.getScmId();

      SCMHAMetrics scmHAMetrics = scm.getScmHAMetrics();
      // If current SCM is leader, state should be 1
      int expectedState = nodeId.equals(leaderSCMId) ? 1 : 0;

      assertEquals(expectedState,
          scmHAMetrics.getSCMHAMetricsInfoLeaderState());
      assertEquals(nodeId, scmHAMetrics.getSCMHAMetricsInfoNodeId());
    }
  }

  @Test
  public void testSCMLeadershipMetric() throws IOException, InterruptedException {
    // GIVEN
    int scmInstancesCount = 3;
    conf = new OzoneConfiguration();
    MiniOzoneHAClusterImpl.Builder haMiniClusterBuilder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId("scm-service-id")
        .setOMServiceId("om-service-id")
        .setNumOfActiveOMs(0)
        .setNumOfStorageContainerManagers(scmInstancesCount)
        .setNumOfActiveSCMs(1);
    haMiniClusterBuilder.setNumDatanodes(0);

    // start single SCM instance without other Ozone services
    // in order to initialize and bootstrap SCM instances only
    cluster = haMiniClusterBuilder.build();

    List<StorageContainerManager> storageContainerManagersList = cluster.getStorageContainerManagersList();

    // stop the single SCM instance in order to imitate further simultaneous start of SCMs
    storageContainerManagersList.get(0).stop();
    storageContainerManagersList.get(0).join();

    // WHEN (imitate simultaneous start of the SCMs)
    int retryCount = 0;
    while (true) {
      CountDownLatch scmInstancesCounter = new CountDownLatch(scmInstancesCount);
      AtomicInteger failedSCMs = new AtomicInteger();
      for (StorageContainerManager scm : storageContainerManagersList) {
        new Thread(() -> {
          try {
            scm.start();
          } catch (IOException e) {
            failedSCMs.incrementAndGet();
          } finally {
            scmInstancesCounter.countDown();
          }
        }).start();
      }
      scmInstancesCounter.await();
      if (failedSCMs.get() == 0) {
        break;
      } else {
        for (StorageContainerManager scm : storageContainerManagersList) {
          scm.stop();
          scm.join();
          LOG.info("Stopping StorageContainerManager server at {}",
              scm.getClientRpcAddress());
        }
        ++retryCount;
        LOG.info("SCMs port conflicts, retried {} times",
            retryCount);
        failedSCMs.set(0);
      }
    }

    // THEN expect only one SCM node (leader) will have 'scmha_metrics_scmha_leader_state' metric set to 1
    int leaderCount = 0;
    for (StorageContainerManager scm : storageContainerManagersList) {
      if (scm.getScmHAMetrics() != null && scm.getScmHAMetrics().getSCMHAMetricsInfoLeaderState() == 1) {
        leaderCount++;
        break;
      }
    }
    assertEquals(1, leaderCount);
  }

}
