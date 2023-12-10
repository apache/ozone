/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.OzoneConsts.LAYOUT_VERSION_KEY;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.assertClusterPrepared;
import static org.apache.hadoop.ozone.om.OMUpgradeTestUtils.waitForFinalization;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager.maxLayoutVersion;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

/**
 * Tests for OM upgrade finalization.
 * TODO: can be merged into class with other OM tests with per-method cluster
 */
class TestOMUpgradeFinalization {

  @Test
  void testOMUpgradeFinalizationWithOneOMDown() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    try (MiniOzoneHAClusterImpl cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();

      try (OzoneClient client = cluster.newClient()) {
        List<OzoneManager> runningOms = cluster.getOzoneManagersList();
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

        omClient.cancelOzoneManagerPrepare();
        StatusAndMessages response =
            omClient.finalizeUpgrade("finalize-test");
        System.out.println("Finalization Messages : " + response.msgs());

        waitForFinalization(omClient);
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

        assertEquals(maxLayoutVersion(),
            downedOM.getVersionManager().getMetadataLayoutVersion());
        String lvString = downedOM.getMetadataManager().getMetaTable()
            .get(LAYOUT_VERSION_KEY);
        assertNotNull(lvString);
        assertEquals(maxLayoutVersion(), Integer.parseInt(lvString));
      }
    }
  }

  private static MiniOzoneHAClusterImpl newCluster(OzoneConfiguration conf)
      throws IOException {
    return (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setClusterId(UUID.randomUUID().toString())
        .setScmId(UUID.randomUUID().toString())
        .setOMServiceId(UUID.randomUUID().toString())
        .setNumOfOzoneManagers(3)
        .setNumDatanodes(1)
        .setOmLayoutVersion(INITIAL_VERSION.layoutVersion())
        .build();
  }

}
