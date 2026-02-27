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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
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

  public static void waitForFinalizationFromClient(
      StorageContainerLocationProtocol scmClient, String clientID)
      throws Exception {
    LambdaTestUtils.await(60_000, 1_000, () -> {
      UpgradeFinalization.Status status = scmClient
          .queryUpgradeFinalizationProgress(clientID, true, true)
          .status();
      LOG.info("Waiting for upgrade finalization to complete from client." +
          " Current status is {}.", status);
      return status == FINALIZATION_DONE || status == ALREADY_FINALIZED;
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
      List<StorageContainerManager> scms, int numContainers, int numDatanodes) {
    for (StorageContainerManager scm : scms) {
      LOG.info("Testing post upgrade conditions on SCM with node ID: {}",
          scm.getSCMNodeId());
      testPostUpgradeConditionsSCM(scm, numContainers, numDatanodes);
    }
  }

  public static void testPostUpgradeConditionsSCM(StorageContainerManager scm,
                                                  int numContainers, int numDatanodes) {

    assertTrue(scm.getScmContext().getFinalizationCheckpoint()
        .hasCrossed(FinalizationCheckpoint.FINALIZATION_COMPLETE));

    HDDSLayoutVersionManager scmVersionManager = scm.getLayoutVersionManager();
    assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    assertThat(scmVersionManager.getMetadataLayoutVersion()).isGreaterThanOrEqualTo(1);

    int countContainers = 0;
    for (ContainerInfo ignored : scm.getContainerManager().getContainers()) {
      countContainers++;
    }
    assertThat(countContainers).isGreaterThanOrEqualTo(numContainers);
  }

  /*
   * Helper function to test Pre-Upgrade conditions on all the DataNodes.
   */
  public static void testPreUpgradeConditionsDataNodes(
      List<HddsDatanodeService> datanodes) {
    for (HddsDatanodeService dataNode : datanodes) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      assertEquals(0, dnVersionManager.getMetadataLayoutVersion());
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : datanodes) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      // Also verify that all the existing containers are open.
      for (Container<?> container :
          dsm.getContainer().getController().getContainers()) {
        assertSame(container.getContainerState(),
            ContainerProtos.ContainerDataProto.State.OPEN);
        countContainers++;
      }
    }
    assertThat(countContainers).isGreaterThanOrEqualTo(1);
  }

  /*
   * Helper function to test Post-Upgrade conditions on all the DataNodes.
   */
  public static void testPostUpgradeConditionsDataNodes(
      List<HddsDatanodeService> datanodes, int numContainers,
      ContainerProtos.ContainerDataProto.State... validClosedContainerStates) {
    try {
      GenericTestUtils.waitFor(() -> {
        for (HddsDatanodeService dataNode : datanodes) {
          DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
          try {
            if ((dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) &&
                (dsm.queryUpgradeStatus().status() != ALREADY_FINALIZED)) {
              return false;
            }
          } catch (IOException e) {
            LOG.error("Failed to query datanode upgrade status.", e);
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
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      assertEquals(dnVersionManager.getSoftwareLayoutVersion(),
          dnVersionManager.getMetadataLayoutVersion());
      assertThat(dnVersionManager.getMetadataLayoutVersion()).isGreaterThanOrEqualTo(1);

      // Verify containers are in acceptable states (OPEN is now allowed).
      for (Container<?> ignored :
          dsm.getContainer().getController().getContainers()) {
        countContainers++;
      }
    }
    assertThat(countContainers).isGreaterThanOrEqualTo(numContainers);
  }

  public static void testDataNodesStateOnSCM(List<StorageContainerManager> scms,
      int expectedDatanodeCount, HddsProtos.NodeState state) {
    scms.forEach(scm -> testDataNodesStateOnSCM(scm, expectedDatanodeCount, state));
  }

  /*
   * Helper function to test DataNode state on the SCM. Note that due to
   * timing constraints, sometime the node-state can transition to the next
   * state. This function expects the DataNode to be in NodeState "state" or
   * "alternateState". Some tests can enforce a unique NodeState test by
   * setting "alternateState = null".
   */
  public static void testDataNodesStateOnSCM(StorageContainerManager scm,
      int expectedDatanodeCount, HddsProtos.NodeState state) {
    int countNodes = 0;
    for (DatanodeDetails dn : scm.getScmNodeManager().getAllNodes()) {
      try {
        HddsProtos.NodeState dnState =
            scm.getScmNodeManager().getNodeStatus(dn).getHealth();
        assertSame(dnState, state);
      } catch (NodeNotFoundException e) {
        e.printStackTrace();
        fail("Node not found");
      }
      ++countNodes;
    }
    assertEquals(expectedDatanodeCount, countNodes);
  }
}
