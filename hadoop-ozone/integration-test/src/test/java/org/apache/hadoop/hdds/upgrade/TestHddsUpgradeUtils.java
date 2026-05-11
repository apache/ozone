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

import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_DONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for testing HDDS upgrade finalization in integration tests.
 */
public final class TestHddsUpgradeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestHddsUpgradeUtils.class);

  private TestHddsUpgradeUtils() { }

  public static void waitForFinalizationFromClient(StorageContainerLocationProtocol scmClient) throws Exception {
    LambdaTestUtils.await(60_000, 1_000, () -> {
      HddsProtos.UpgradeStatus status = scmClient.queryUpgradeStatus();
      LOG.info("Waiting for upgrade finalization to complete from client. Current status is:\n{}", status);
      return status.getShouldFinalize();
    });
  }

  /*
   * Helper function to test Pre-Upgrade conditions on the SCM
   */
  public static void testPreUpgradeConditionsSCM(
      List<StorageContainerManager> scms) {
    for (StorageContainerManager scm : scms) {
      assertEquals(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(),
          scm.getLayoutVersionManager().getMetadataLayoutVersion());
      for (ContainerInfo ci : scm.getContainerManager()
          .getContainers()) {
        assertEquals(HddsProtos.LifeCycleState.OPEN, ci.getState());
      }
    }
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
    HDDSLayoutVersionManager scmVersionManager = scm.getLayoutVersionManager();
    assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    assertThat(scmVersionManager.getMetadataLayoutVersion()).isGreaterThanOrEqualTo(1);

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
          DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
          if ((dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) &&
              (dsm.queryUpgradeStatus().status() != ALREADY_FINALIZED)) {
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
}
