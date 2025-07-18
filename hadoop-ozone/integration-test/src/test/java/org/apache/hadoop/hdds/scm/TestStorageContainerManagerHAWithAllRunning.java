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
import static org.apache.ozone.test.GenericTestUtils.getTestStartTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.ha.SCMHAMetrics;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.TestDataUtil;
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
import org.apache.ozone.test.HATests;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for SCM HA.
 */
public abstract class TestStorageContainerManagerHAWithAllRunning implements HATests.TestCase {

  private static final int OM_COUNT = 3;
  private static final int SCM_COUNT = 3;
    // conf.set(ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL, "5s");
    // conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, "1");

  @Test
  void testAllSCMAreRunning() throws Exception {
    List<StorageContainerManager> scms = cluster().getStorageContainerManagers();
    assertEquals(SCM_COUNT, scms.size());
    int peerSize = cluster().getStorageContainerManager().getScmHAManager()
        .getRatisServer().getDivision().getGroup().getPeers().size();
    int scmLeadersFound = 0;
    StorageContainerManager leaderScm = null;
    for (StorageContainerManager scm : scms) {
      if (scm.checkLeader()) {
        scmLeadersFound++;
        leaderScm = scm;
      }
      assertNotNull(scm.getScmHAManager().getRatisServer().getLeaderId());
      assertEquals(peerSize, SCM_COUNT);
    }
    assertEquals(1, scmLeadersFound);
    assertNotNull(leaderScm);

    assertRatisRoles();

    String leaderSCMId = leaderScm.getScmId();
    checkSCMHAMetricsForAllSCMs(scms, leaderSCMId);

    List<OzoneManager> oms = cluster().getOzoneManagersList();
    assertEquals(OM_COUNT, oms.size());
    int omLeadersFound = 0;
    for (OzoneManager om : oms) {
      if (om.isLeaderReady()) {
        omLeadersFound++;
      }
    }
    assertEquals(1, omLeadersFound);
    
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
    try (OzoneClient client = cluster().newClient()) {
      ObjectStore store = client.getObjectStore();
      String value = "sample value";
      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);

      String keyName = UUID.randomUUID().toString();

      byte[] bytes = value.getBytes(UTF_8);
      RatisReplicationConfig replication = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);
      TestDataUtil.createKey(bucket, keyName, replication, bytes);

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
          .setReplicationConfig(replication)
          .setKeyName(keyName)
          .build();
      final OmKeyInfo keyInfo = cluster().getOMLeader().lookupKey(keyArgs);
      final List<OmKeyLocationInfo> keyLocationInfos =
          keyInfo.getKeyLocationVersions().get(0).getBlocksLatestVersionOnly();
      long index = -1;
      for (StorageContainerManager scm : cluster().getStorageContainerManagers()) {
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
            cluster().getStorageContainerManagers().get(k);
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
    List<StorageContainerManager> scms = cluster().getStorageContainerManagers();
    boolean sync = false;
    for (StorageContainerManager scm : scms) {
      sync = getLastAppliedIndex(scm) == leaderIndex;
    }
    return sync;
  }

  private void assertRatisRoles() {
    Set<String> resultSet = new HashSet<>();
    for (StorageContainerManager scm: cluster().getStorageContainerManagers()) {
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

}
