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

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.audit.OMSystemAction;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OM upgrade finalization.
 * TODO: can be merged into class with other OM tests with per-method cluster
 */
class TestOMUpgradeFinalization {
  private static final int LOG_PURGE_GAP = 1;
  private static final long SNAPSHOT_THRESHOLD = 1;

  static {
    AuditLogTestUtils.enableAuditLog();
  }

  @BeforeEach
  public void setup() throws Exception {
    AuditLogTestUtils.truncateAuditLogFile();
  }

  @AfterAll
  public static void shutdown() {
    AuditLogTestUtils.deleteAuditLogFile();
  }

  @Test
  void testFinalization2() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();
      // Get the leader OM
      final String leaderOMNodeId = OmTestUtil.getCurrentOmProxyNodeId(cluster.newClient().getObjectStore());

      OzoneManager leaderOM = cluster.getOzoneManager(leaderOMNodeId);

      // Find the inactive OM
      String followerNodeId = leaderOM.getPeerNodes().get(0).getNodeId();
      if (cluster.isOMActive(followerNodeId)) {
        followerNodeId = leaderOM.getPeerNodes().get(1).getNodeId();
      }
//      OzoneManager followerOM = cluster.getOzoneManager(followerNodeId);

      try (OzoneClient client = cluster.newClient()) {
        for (int i = 0; i < 100; i++) {
          client.getObjectStore().createVolume("vol" + i);
        }
      }

//      // Get the latest db checkpoint from the leader OM.
//      TransactionInfo transactionInfo =
//          TransactionInfo.readTransactionInfo(leaderOM.getMetadataManager());
//      TermIndex leaderOMTermIndex =
//          TermIndex.valueOf(transactionInfo.getTerm(),
//              transactionInfo.getTransactionIndex());
//      long leaderOMSnapshotIndex = leaderOMTermIndex.getIndex();
//      long leaderOMSnapshotTermIndex = leaderOMTermIndex.getTerm();

      // Start the inactive OM. Checkpoint installation will happen spontaneously.
      cluster.startInactiveOM(followerNodeId);

      // The recently started OM should be lagging behind the leader OM.
      // Wait & for follower to update transactions to leader snapshot index.
      // Timeout error if follower does not load update within 10s
//      GenericTestUtils.waitFor(() -> {
//        long index = followerOM.getOmRatisServer().getLastAppliedTermIndex().getIndex();
//        return index >= leaderOMSnapshotIndex - 1;
//      }, 100, 30_000);

      // Verify the inactive OM installed a snapshot from the leader.
      AuditLogTestUtils.verifyAuditLog(OMSystemAction.DB_CHECKPOINT_INSTALL,
          AuditEventStatus.SUCCESS);
    }
  }

  @Test
  void testOMUpgradeFinalizationFromSnapshot() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();

      try (OzoneClient client = cluster.newClient()) {
        // One OM was never started (setNumOfActiveOMs(2)); find it.
        OzoneManager inactiveOM = cluster.getOzoneManagersList().stream()
            .filter(om -> !cluster.isOMActive(om.getOMNodeId()))
            .findFirst()
            .get();

        OzoneManagerProtocol omClient = client.getObjectStore().getClientProxy()
            .getOzoneManagerClient();

        StatusAndMessages response =
            omClient.finalizeUpgrade("finalize-test");
        System.out.println("Finalization Messages : " + response.msgs());
        AuditLogTestUtils.verifyAuditLog(OMAction.UPGRADE_FINALIZE, AuditEventStatus.SUCCESS);

        waitForFinalization(omClient);

        // The finalization log entries, combined with SNAPSHOT_THRESHOLD=1 and
        // LOG_PURGE_GAP=1, are sufficient to trigger a snapshot and purge the
        // logs the inactive OM never received, forcing it to install a snapshot.

        cluster.startInactiveOM(inactiveOM.getOMNodeId());

        // Verify the inactive OM installed a snapshot from the leader.
        AuditLogTestUtils.verifyAuditLog(OMSystemAction.DB_CHECKPOINT_INSTALL,
            AuditEventStatus.SUCCESS);

        assertEquals(maxLayoutVersion(),
            inactiveOM.getVersionManager().getMetadataLayoutVersion());
        String lvString = inactiveOM.getMetadataManager().getMetaTable()
            .get(LAYOUT_VERSION_KEY);
        assertNotNull(lvString);
        assertEquals(maxLayoutVersion(), Integer.parseInt(lvString));
      }
    }
  }

  private static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf)
      throws IOException {
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, INITIAL_VERSION.layoutVersion());
    conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, LOG_PURGE_GAP);
    conf.setLong(OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY, SNAPSHOT_THRESHOLD);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_SIZE_KEY, 16, StorageUnit.KB);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY, 16, StorageUnit.KB);
    OzoneManagerRatisServerConfig omRatisConf =
        conf.getObject(OzoneManagerRatisServerConfig.class);
    omRatisConf.setLogAppenderWaitTimeMin(10);
    conf.setFromObject(omRatisConf);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(UUID.randomUUID().toString())
        .setNumOfOzoneManagers(3)
        .setNumOfActiveOMs(2)
        .setNumDatanodes(1);
    return builder.build();
  }

}
