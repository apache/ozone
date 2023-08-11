/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.upgrade;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationCheckpoint;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY_READONLY;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.awaitility.Awaitility.await;

/**
 * Helper methods for testing HDDS upgrade finalization in integration tests.
 */
public final class TestHddsUpgradeUtils {

  private TestHddsUpgradeUtils() { }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHddsUpgradeUtils.class);

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);

  public static void waitForFinalizationFromClient(
      StorageContainerLocationProtocol scmClient,
      String clientID
  ) {
    await().atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofSeconds(1))
        .until(() -> {
          UpgradeFinalizer.Status status = scmClient
              .queryUpgradeFinalizationProgress(clientID, true, true)
              .status();
          return status == FINALIZATION_DONE || status == ALREADY_FINALIZED;
        });
  }

  /*
   * Helper function to test Pre-Upgrade conditions on the SCM
   */
  public static void testPreUpgradeConditionsSCM(
      List<StorageContainerManager> scms) {
    for (StorageContainerManager scm : scms) {
      Assert.assertEquals(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion(),
          scm.getLayoutVersionManager().getMetadataLayoutVersion());
      for (ContainerInfo ci : scm.getContainerManager()
          .getContainers()) {
        Assert.assertEquals(HddsProtos.LifeCycleState.OPEN, ci.getState());
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

    Assert.assertTrue(scm.getScmContext().getFinalizationCheckpoint()
        .hasCrossed(FinalizationCheckpoint.FINALIZATION_COMPLETE));

    HDDSLayoutVersionManager scmVersionManager = scm.getLayoutVersionManager();
    Assert.assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    Assert.assertTrue(scmVersionManager.getMetadataLayoutVersion() >= 1);

    // SCM should not return from finalization until there is at least one
    // pipeline to use.
    PipelineManager scmPipelineManager = scm.getPipelineManager();
    await().atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .until(() ->
            scmPipelineManager.getPipelines(RATIS_THREE, OPEN).size() >= 1);

    // SCM will not return from finalization until there is at least one
    // RATIS 3 pipeline. For this to exist, all three of our datanodes must
    // be in the HEALTHY state.
    testDataNodesStateOnSCM(scm, numDatanodes, HEALTHY, HEALTHY_READONLY);

    int countContainers = 0;
    for (ContainerInfo ci : scm.getContainerManager().getContainers()) {
      HddsProtos.LifeCycleState ciState = ci.getState();
      LOG.info("testPostUpgradeConditionsSCM: container state is {}",
          ciState.name());
      Assert.assertTrue((ciState == HddsProtos.LifeCycleState.CLOSED) ||
          (ciState == HddsProtos.LifeCycleState.CLOSING) ||
          (ciState == HddsProtos.LifeCycleState.DELETING) ||
          (ciState == HddsProtos.LifeCycleState.DELETED) ||
          (ciState == HddsProtos.LifeCycleState.QUASI_CLOSED));
      countContainers++;
    }
    Assert.assertTrue(countContainers >= numContainers);
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
      Assert.assertEquals(0, dnVersionManager.getMetadataLayoutVersion());
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : datanodes) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      // Also verify that all the existing containers are open.
      for (Container<?> container :
          dsm.getContainer().getController().getContainers()) {
        Assert.assertSame(container.getContainerState(),
            ContainerProtos.ContainerDataProto.State.OPEN);
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= 1);
  }

  /*
   * Helper function to test Post-Upgrade conditions on all the DataNodes.
   */
  public static void testPostUpgradeConditionsDataNodes(
      List<HddsDatanodeService> datanodes, int numContainers,
      ContainerProtos.ContainerDataProto.State... validClosedContainerStates) {
    List<ContainerProtos.ContainerDataProto.State> closeStates =
        Arrays.asList(validClosedContainerStates);
    // Allow closed and quasi closed containers as valid closed containers by
    // default.
    if (closeStates.isEmpty()) {
      closeStates = Arrays.asList(CLOSED, QUASI_CLOSED);
    }

    await().atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .ignoreException(IOException.class)
        .until(() -> {
          for (HddsDatanodeService dataNode : datanodes) {
            DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
            if ((dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) &&
                (dsm.queryUpgradeStatus().status() != ALREADY_FINALIZED)) {
              return false;
            }
          }
          return true;
        });

    int countContainers = 0;
    for (HddsDatanodeService dataNode : datanodes) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      Assert.assertEquals(dnVersionManager.getSoftwareLayoutVersion(),
          dnVersionManager.getMetadataLayoutVersion());
      Assert.assertTrue(dnVersionManager.getMetadataLayoutVersion() >= 1);

      // Also verify that all the existing containers are closed.
      for (Container<?> container :
           dsm.getContainer().getController().getContainers()) {
        Assert.assertTrue("Container had unexpected state " +
                container.getContainerState(),
            closeStates.stream().anyMatch(
                state -> container.getContainerState().equals(state)));
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= numContainers);
  }

  public static void testDataNodesStateOnSCM(List<StorageContainerManager> scms,
      int expectedDatanodeCount, HddsProtos.NodeState state,
      HddsProtos.NodeState alternateState) {
    scms.forEach(scm -> testDataNodesStateOnSCM(scm, expectedDatanodeCount,
        state, alternateState));
  }

  /*
   * Helper function to test DataNode state on the SCM. Note that due to
   * timing constraints, sometime the node-state can transition to the next
   * state. This function expects the DataNode to be in NodeState "state" or
   * "alternateState". Some tests can enforce a unique NodeState test by
   * setting "alternateState = null".
   */
  public static void testDataNodesStateOnSCM(StorageContainerManager scm,
      int expectedDatanodeCount, HddsProtos.NodeState state,
      HddsProtos.NodeState alternateState) {
    int countNodes = 0;
    for (DatanodeDetails dn : scm.getScmNodeManager().getAllNodes()) {
      try {
        HddsProtos.NodeState dnState =
            scm.getScmNodeManager().getNodeStatus(dn).getHealth();
        Assert.assertTrue((dnState == state) ||
            (alternateState != null && dnState == alternateState));
      } catch (NodeNotFoundException e) {
        e.printStackTrace();
        Assert.fail("Node not found");
      }
      ++countNodes;
    }
    Assert.assertEquals(expectedDatanodeCount, countNodes);
  }
}
