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

import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the finalize upgrade RPCs sent by old CLIs still finalize a new server.
 *
 * <p>In the new finalization model a single call to OM drives the whole cluster: OM triggers SCM
 * finalization and then finalizes itself once SCM reports done. Old CLIs predate this and issue
 * the deprecated {@code FinalizeUpgrade} RPC to OM (and could target SCM directly). The server must
 * keep honoring those RPCs.
 */
class TestUpgradeFinalizationWithOldClients {

  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeFinalizationWithOldClients.class);

  /**
   * An old {@code ozone admin om finalizeupgrade} CLI issues the deprecated {@code FinalizeUpgrade}
   * RPC via {@link OzoneManagerProtocol#finalizeUpgrade(String)} and then polls
   * {@link OzoneManagerProtocol#queryUpgradeFinalizationProgress(String, boolean, boolean)} until
   * finalization is done. This drives the whole cluster: OM, SCM, and the datanode all start
   * pre-finalized and end finalized.
   *
   * <p>It also exercises the deprecated SCM finalize API used by an old
   * {@code ozone admin scm finalizeupgrade} CLI: finalizing SCM directly is now a no-op (SCM
   * finalization is driven from OM), while SCM's finalization status remains individually queryable.
   */
  @Test
  void testFinalizeUpgradeWithOldClientRpcs() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_UPGRADE_FINALIZATION_CHECK_INTERVAL, "10ms");
    // Start OM, SCM, and the datanode pre-finalized: new binaries sitting on the initial apparent version,
    conf.setInt(OMStorage.TESTING_INIT_APPARENT_VERSION_KEY, OMLayoutFeature.INITIAL_VERSION.serialize());
    conf.setInt(SCMStorageConfig.TESTING_INIT_APPARENT_VERSION_KEY, HDDSLayoutFeature.INITIAL_VERSION.serialize());

    try (MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setApparentVersion(HDDSLayoutFeature.INITIAL_VERSION.serialize())
            .build())
        .build()) {
      cluster.waitForClusterToBeReady();
      OzoneManager om = cluster.getOzoneManager();
      StorageContainerManager scm = cluster.getStorageContainerManager();
      StorageContainerLocationProtocol scmClient = cluster.getStorageContainerLocationClient();

      try (OzoneClient client = cluster.newClient()) {
        OzoneManagerProtocol omClient = client.getObjectStore().getClientProxy().getOzoneManagerClient();

        // OM, SCM, and the datanode all start pre-finalized (needing finalization). The datanode's
        // state is read directly off the running datanode instance rather than through a client RPC.
        assertEquals(OMLayoutFeature.INITIAL_VERSION, om.getVersionManager().getApparentVersion());
        assertTrue(om.getVersionManager().needsFinalization());
        assertEquals(HDDSLayoutFeature.INITIAL_VERSION, scm.getVersionManager().getApparentVersion());
        assertTrue(scm.getVersionManager().needsFinalization());
        for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
          DatanodeVersionManager dnVersionManager = dn.getDatanodeStateMachine().getVersionManager();
          assertEquals(HDDSLayoutFeature.INITIAL_VERSION, dnVersionManager.getApparentVersion());
          assertTrue(dnVersionManager.needsFinalization());
        }

        // SCM's finalization status is individually queryable through the deprecated SCM API.
        StatusAndMessages scmProgressBefore = scmClient.queryUpgradeFinalizationProgress(
            "Upgrade-Client-" + UUID.randomUUID(), false, false);
        assertEquals(UpgradeFinalization.Status.FINALIZATION_REQUIRED, scmProgressBefore.status());

        // Finalizing SCM directly via the deprecated API is a no-op: it reports ALREADY_FINALIZED to
        // let old scripts move on, but it does not actually finalize SCM (that is driven from OM).
        StatusAndMessages scmFinalizeResponse =
            scmClient.finalizeScmUpgrade("Upgrade-Client-" + UUID.randomUUID());
        assertTrue(UpgradeFinalization.isFinalized(scmFinalizeResponse.status()),
            "Expected ALREADY_FINALIZED but got " + scmFinalizeResponse.status());
        assertTrue(scm.getVersionManager().needsFinalization(),
            "Finalizing SCM directly through the legacy API must not finalize SCM");
        assertEquals(UpgradeFinalization.Status.FINALIZATION_REQUIRED,
            scmClient.queryUpgradeFinalizationProgress("Upgrade-Client-" + UUID.randomUUID(), false, false).status());

        // The old CLI initiates finalization with the deprecated FinalizeUpgrade RPC. The server
        // reports STARTING_FINALIZATION so the old CLI proceeds to monitor progress.
        String upgradeClientID = "Upgrade-Client-" + UUID.randomUUID();
        StatusAndMessages started = omClient.finalizeUpgrade(upgradeClientID);
        assertTrue(UpgradeFinalization.isStarting(started.status()),
            "Expected STARTING_FINALIZATION but got " + started.status());

        // The old CLI polls progress until finalization is done, exactly as FinalizeUpgradeSubCommand does.
        waitFor(() -> {
          try {
            StatusAndMessages progress =
                omClient.queryUpgradeFinalizationProgress(upgradeClientID, false, false);
            LOG.info("Waiting for OM finalization to finish. Current status: {}", progress.status());
            return UpgradeFinalization.isDone(progress.status());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }, 2000, 60000);

        // OM and SCM are finalized: OM advanced to the software version and both report FINALIZED.
        assertEquals(OzoneManagerVersion.SOFTWARE_VERSION, om.getVersionManager().getApparentVersion());
        assertEquals(HDDSVersion.SOFTWARE_VERSION, scm.getVersionManager().getApparentVersion());

        // SCM now reports done through the deprecated SCM API too.
        assertTrue(UpgradeFinalization.isDone(
            scmClient.queryUpgradeFinalizationProgress(upgradeClientID, false, false).status()));

        // Server side enforces finalization order of SCM->DNs->OM. Since we already waited for OM to finalize,
        // Datanodes should now be finalized.
        for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
          DatanodeVersionManager dnVersionManager = dn.getDatanodeStateMachine().getVersionManager();
          assertFalse(dnVersionManager.needsFinalization());
          assertEquals(HDDSVersion.SOFTWARE_VERSION, dnVersionManager.getApparentVersion());
        }
      }
    }
  }
}
