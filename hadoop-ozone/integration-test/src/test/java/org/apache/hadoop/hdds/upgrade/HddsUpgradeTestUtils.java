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

package org.apache.hadoop.hdds.upgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for testing HDDS upgrade finalization in integration tests.
 */
public final class HddsUpgradeTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HddsUpgradeTestUtils.class);

  private HddsUpgradeTestUtils() { }

  public static void waitForFinalizationFromClient(StorageContainerLocationProtocol scmClient) throws Exception {
    LambdaTestUtils.await(60_000, 1_000, () -> {
      HddsProtos.UpgradeStatus status = scmClient.queryUpgradeStatus();
      LOG.info("Waiting for upgrade finalization to complete from client. Current status is:\n{}", status);
      return status.getHddsFinalized();
    });
  }

  /*
   * Helper function to test Post-Upgrade conditions on the SCM
   */
  public static void testPostUpgradeConditionsSCM(
      List<StorageContainerManager> scms, int numContainers) {
    for (StorageContainerManager scm : scms) {
      LOG.info("Testing post upgrade conditions on SCM with node ID: {}",
          scm.getSCMNodeId());
      testPostUpgradeConditionsSCM(scm, numContainers);
    }
  }

  public static void testPostUpgradeConditionsSCM(StorageContainerManager scm,
                                                  int numContainers) {
    assertEquals(scm.getVersionManager().getSoftwareVersion(),
        scm.getVersionManager().getApparentVersion());
    assertThat(scm.getVersionManager().getApparentVersion().serialize()).isGreaterThanOrEqualTo(1);

    int countContainers = scm.getContainerManager().getContainers().size();
    assertThat(countContainers).isGreaterThanOrEqualTo(numContainers);
  }

  /*
   * Helper function to test Post-Upgrade conditions on all the DataNodes.
   */
  public static void testPostUpgradeConditionsDataNodes(
      List<HddsDatanodeService> datanodes, int numContainers) {
    try {
      GenericTestUtils.waitFor(() -> {
        for (HddsDatanodeService dataNode : datanodes) {
          DatanodeVersionManager versionManager = dataNode.getDatanodeStateMachine().getVersionManager();
          if (versionManager.needsFinalization()) {
            return false;
          }
        }
        return true;
      }, 500, 60000);
    } catch (TimeoutException | InterruptedException e) {
      fail("Timeout waiting for Upgrade to complete on Data Nodes.");
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : datanodes) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      DatanodeVersionManager dnVersionManager =
          dsm.getVersionManager();
      assertEquals(dnVersionManager.getSoftwareVersion(),
          dnVersionManager.getApparentVersion());
      assertThat(dnVersionManager.getApparentVersion().serialize())
          .isGreaterThanOrEqualTo(1);
      countContainers += dsm.getContainer().getContainerSet().containerCount();
    }
    assertThat(countContainers).isGreaterThanOrEqualTo(numContainers);
  }

  public static void waitForScmsToFinalize(Collection<StorageContainerManager> scms)
      throws Exception {
    for (StorageContainerManager scm: scms) {
      // SCM will flush entries to the DB async, the state is kept in memory and the ratis logs.
      // In the common case, the apparent version will not appear in the DB at the time of finalization since the logs
      // Will not have been flushed.
      waitForScmToFinalize(scm, false);
    }
  }

  public static void waitForScmToFinalize(StorageContainerManager scm, boolean waitForDBKeyFlush)
      throws Exception {
    GenericTestUtils.waitFor(() -> isScmFinalized(scm, waitForDBKeyFlush), 2_000, 60_000);
  }

  private static boolean isScmFinalized(StorageContainerManager scm, boolean waitForDBKeyFlush) {
    boolean exitedSafemode = !scm.isInSafeMode();
    boolean isFinalized = !scm.getVersionManager().needsFinalization();
    boolean dbKeyFlushed = false;

    try {
      dbKeyFlushed = scm.getScmMetadataStore().getMetaTable().get(OzoneConsts.APPARENT_VERSION_KEY) != null;
    } catch (RocksDatabaseException | CodecException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Waiting for SCM {} (leader? {}) to finalize.\n" +
            "Exited safemode? {}\n" +
            "version manager finalized? {}\n" +
            "DB key flushed? {}\n" +
            "Requiring DB key to flush? {}",
        scm.getSCMNodeId(), scm.checkLeader(), exitedSafemode, isFinalized, dbKeyFlushed, waitForDBKeyFlush);

    return exitedSafemode && isFinalized && (!waitForDBKeyFlush || dbKeyFlushed);
  }
}
