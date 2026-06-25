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
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.assertClusterPrepared;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogTestUtils;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for OM upgrade finalization.
 * TODO: can be merged into class with other OM tests with per-method cluster
 */
class TestOMUpgradeFinalization {
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
    AuditLogTestUtils.deleteSystemAuditLogFile();
  }

  @Test
  void testOMUpgradeFinalizationWithOneOMDown() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, INITIAL_VERSION.serialize());
    conf.set(OMConfigKeys.OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL, "10ms");
    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();

      try (OzoneClient client = cluster.newClient()) {
        List<OzoneManager> runningOms = cluster.getOzoneManagersList();
        for (OzoneManager om : runningOms) {
          assertEquals(INITIAL_VERSION, om.getVersionManager().getApparentVersion());
          // The OMs have not been finalized yet, so no version has been written to the DB.
          assertNull(om.getMetadataManager().getMetaTable().get(APPARENT_VERSION_KEY));
        }

        final int shutdownOMIndex = 2;
        OzoneManager downedOM = cluster.getOzoneManager(shutdownOMIndex);
        cluster.stopOzoneManager(shutdownOMIndex);
        assertFalse(downedOM.isRunning());
        assertEquals(runningOms.remove(shutdownOMIndex), downedOM);

        OzoneManagerProtocol omClient = client.getObjectStore().getClientProxy()
            .getOzoneManagerClient();
        // Have to do a "prepare" operation to get rid of the logs in the active
        // OMs.
        long prepareIndex = omClient.prepareOzoneManager(120L, 5L);
        assertClusterPrepared(prepareIndex, runningOms);
        AuditLogTestUtils.verifyAuditLog(OMAction.UPGRADE_PREPARE, AuditEventStatus.SUCCESS);
        omClient.cancelOzoneManagerPrepare();
        AuditLogTestUtils.verifyAuditLog(OMAction.UPGRADE_CANCEL, AuditEventStatus.SUCCESS);
        omClient.finalizeUpgrade();
        waitForFinalization(omClient);
        AuditLogTestUtils.verifySystemAuditLog(OMAction.UPGRADE_FINALIZE, AuditEventStatus.SUCCESS);
        // Ensure the finalization in progress key has been removed.
        assertNull(cluster.getOzoneManager().getMetadataManager()
            .getMetaTable().get(OzoneConsts.FINALIZATION_IN_PROGRESS_KEY));
        cluster.restartOzoneManager(downedOM, true);

        OzoneManagerStateMachine omStateMachine = downedOM.getOmRatisServer()
            .getOmStateMachine();
        try {
          waitFor(() -> omStateMachine.getLifeCycleState().isPausingOrPaused(),
              1000, 60000);
        } catch (TimeoutException timeEx) {
          assertEquals(LifeCycle.State.RUNNING,
              omStateMachine.getLifeCycle().getCurrentState());
        }

        waitFor(() -> !omStateMachine.getLifeCycle().getCurrentState()
            .isPausingOrPaused(), 1000, 60000);

        assertEquals(OzoneManagerVersion.SOFTWARE_VERSION, downedOM.getVersionManager().getApparentVersion());
        String dbVersionString = downedOM.getMetadataManager().getMetaTable().get(APPARENT_VERSION_KEY);
        assertNotNull(dbVersionString);
        assertEquals(OzoneManagerVersion.SOFTWARE_VERSION.serialize(), Integer.parseInt(dbVersionString));
      }
    }
  }

  private static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf)
      throws IOException {
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, INITIAL_VERSION.serialize());
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setOMServiceId(UUID.randomUUID().toString())
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1);
    return builder.build();
  }

}
