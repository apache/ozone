/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager.MoveResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ReplicationManager replicationManager;
  private ContainerManager containerManager;
  private ContainerBalancer containerBalancer;
  private MockNodeManager mockNodeManager;
  private StorageContainerManager scm;
  private OzoneConfiguration conf;
  private PlacementPolicy placementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private PlacementPolicyValidateProxy placementPolicyValidateProxy;
  private ContainerBalancerConfiguration balancerConfiguration;
  private List<DatanodeUsageInfo> nodesInCluster;
  private List<Double> nodeUtilizations;
  private double averageUtilization;
  private int numberOfNodes;
  private Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
      new HashMap<>();
  private Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private Map<DatanodeUsageInfo, Set<ContainerID>> datanodeToContainersMap =
      new HashMap<>();
  private Map<String, ByteString> serviceToConfigMap = new HashMap<>();
  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  private StatefulServiceStateManager serviceStateManager;
  private static final long STORAGE_UNIT = OzoneConsts.GB;

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException,
      TimeoutException {
    conf = new OzoneConfiguration();
    scm = Mockito.mock(StorageContainerManager.class);
    containerManager = Mockito.mock(ContainerManager.class);
    replicationManager = Mockito.mock(ReplicationManager.class);
    serviceStateManager = Mockito.mock(StatefulServiceStateManagerImpl.class);
    SCMServiceManager scmServiceManager = Mockito.mock(SCMServiceManager.class);

    // these configs will usually be specified in each test
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancer.LOG, Level.DEBUG);

    averageUtilization = createCluster();
    mockNodeManager = new MockNodeManager(datanodeToContainersMap);

    NetworkTopology clusterMap = mockNodeManager.getClusterNetworkTopologyMap();

    placementPolicy = ContainerPlacementPolicyFactory
        .getPolicy(conf, mockNodeManager, clusterMap, true,
            SCMContainerPlacementMetrics.create());
    ecPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(
        conf, mockNodeManager, clusterMap,
        true, SCMContainerPlacementMetrics.create());
    placementPolicyValidateProxy = new PlacementPolicyValidateProxy(
        placementPolicy, ecPlacementPolicy);

    Mockito.when(replicationManager
        .isContainerReplicatingOrDeleting(Mockito.any(ContainerID.class)))
        .thenReturn(false);

    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
        Mockito.any(DatanodeDetails.class),
        Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(MoveResult.COMPLETED));

    when(containerManager.getContainerReplicas(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cidToReplicasMap.get(cid);
        });

    when(containerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cidToInfoMap.get(cid);
        });

    when(containerManager.getContainers())
        .thenReturn(new ArrayList<>(cidToInfoMap.values()));

    when(scm.getScmNodeManager()).thenReturn(mockNodeManager);
    when(scm.getContainerPlacementPolicy()).thenReturn(placementPolicy);
    when(scm.getContainerManager()).thenReturn(containerManager);
    when(scm.getReplicationManager()).thenReturn(replicationManager);
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
    when(scm.getClusterMap()).thenReturn(null);
    when(scm.getEventQueue()).thenReturn(mock(EventPublisher.class));
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getStatefulServiceStateManager()).thenReturn(serviceStateManager);
    when(scm.getSCMServiceManager()).thenReturn(scmServiceManager);
    when(scm.getPlacementPolicyValidateProxy())
        .thenReturn(placementPolicyValidateProxy);

    /*
    When StatefulServiceStateManager#saveConfiguration is called, save to
    in-memory serviceToConfigMap instead.
     */
    Mockito.doAnswer(i -> {
      serviceToConfigMap.put(i.getArgument(0, String.class), i.getArgument(1,
          ByteString.class));
      return null;
    }).when(serviceStateManager).saveConfiguration(
        Mockito.any(String.class),
        Mockito.any(ByteString.class));

    /*
    When StatefulServiceStateManager#readConfiguration is called, read from
    serviceToConfigMap instead.
     */
    when(serviceStateManager.readConfiguration(Mockito.anyString())).thenAnswer(
        i -> serviceToConfigMap.get(i.getArgument(0, String.class)));

    Mockito.doNothing().when(scmServiceManager)
        .register(Mockito.any(SCMService.class));
    containerBalancer = new ContainerBalancer(scm);
  }

  @Test
  public void testCalculationOfUtilization() {
    Assertions.assertEquals(nodesInCluster.size(), nodeUtilizations.size());
    for (int i = 0; i < nodesInCluster.size(); i++) {
      Assertions.assertEquals(nodeUtilizations.get(i),
          nodesInCluster.get(i).calculateUtilization(), 0.0001);
    }

    // should be equal to average utilization of the cluster
    Assertions.assertEquals(averageUtilization,
        containerBalancer.calculateAvgUtilization(nodesInCluster), 0.0001);
  }

  /**
   * Checks whether ContainerBalancer is correctly updating the list of
   * unBalanced nodes with varying values of Threshold.
   */
  @Test
  public void
      initializeIterationShouldUpdateUnBalancedNodesWhenThresholdChanges()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    List<DatanodeUsageInfo> expectedUnBalancedNodes;
    List<DatanodeUsageInfo> unBalancedNodesAccordingToBalancer;

    // check for random threshold values
    for (int i = 0; i < 50; i++) {
      double randomThreshold = RANDOM.nextDouble() * 100;

      balancerConfiguration.setThreshold(randomThreshold);
      startBalancer(balancerConfiguration);

      // waiting for balance completed.
      // TODO: this is a temporary implementation for now
      // modify this after balancer is fully completed
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) { }

      expectedUnBalancedNodes =
          determineExpectedUnBalancedNodes(randomThreshold);
      unBalancedNodesAccordingToBalancer =
          containerBalancer.getUnBalancedNodes();

      stopBalancer();
      Assertions.assertEquals(
          expectedUnBalancedNodes.size(),
          unBalancedNodesAccordingToBalancer.size());

      for (int j = 0; j < expectedUnBalancedNodes.size(); j++) {
        Assertions.assertEquals(
            expectedUnBalancedNodes.get(j).getDatanodeDetails(),
            unBalancedNodesAccordingToBalancer.get(j).getDatanodeDetails());
      }
    }
  }

  /**
   * Checks whether the list of unBalanced nodes is empty when the cluster is
   * balanced.
   */
  @Test
  public void unBalancedNodesListShouldBeEmptyWhenClusterIsBalanced()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(99.99);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(100);

    stopBalancer();
    ContainerBalancerMetrics metrics = containerBalancer.getMetrics();
    Assertions.assertEquals(0, containerBalancer.getUnBalancedNodes().size());
    Assertions.assertEquals(0, metrics.getNumDatanodesUnbalanced());
  }

  /**
   * ContainerBalancer should not involve more datanodes than the
   * maxDatanodesRatioToInvolvePerIteration limit.
   */
  @Test
  public void containerBalancerShouldObeyMaxDatanodesToInvolveLimit()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    int percent = 20;
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(
        percent);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    int number = percent * numberOfNodes / 100;
    ContainerBalancerMetrics metrics = containerBalancer.getMetrics();
    Assertions.assertFalse(
        containerBalancer.getCountDatanodesInvolvedPerIteration() > number);
    Assertions.assertTrue(
        metrics.getNumDatanodesInvolvedInLatestIteration() > 0);
    Assertions.assertFalse(
        metrics.getNumDatanodesInvolvedInLatestIteration() > number);
    stopBalancer();
  }

  @Test
  public void containerBalancerShouldSelectOnlyClosedContainers()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    // make all containers open, balancer should not select any of them
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.OPEN);
    }
    balancerConfiguration.setThreshold(10);
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);
    stopBalancer();

    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    // no container should have been selected
    Assertions.assertTrue(containerBalancer.getContainerToSourceMap()
        .isEmpty());
    /*
    Iteration result should be CAN_NOT_BALANCE_ANY_MORE because no container
    move is generated
     */
    Assertions.assertEquals(
        ContainerBalancer.IterationResult.CAN_NOT_BALANCE_ANY_MORE,
        containerBalancer.getIterationResult());

    // now, close all containers
    for (ContainerInfo containerInfo : cidToInfoMap.values()) {
      containerInfo.setState(HddsProtos.LifeCycleState.CLOSED);
    }
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);
    stopBalancer();

    // check whether all selected containers are closed
    for (ContainerID cid:
         containerBalancer.getContainerToSourceMap().keySet()) {
      Assertions.assertSame(
          cidToInfoMap.get(cid).getState(), HddsProtos.LifeCycleState.CLOSED);
    }
  }

  @Test
  public void containerBalancerShouldObeyMaxSizeToMoveLimit()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(1);
    balancerConfiguration.setMaxSizeToMovePerIteration(10 * STORAGE_UNIT);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    // balancer should not have moved more size than the limit
    Assertions.assertFalse(
        containerBalancer.getSizeScheduledForMoveInLatestIteration() >
        10 * STORAGE_UNIT);

    long size = containerBalancer.getMetrics()
        .getDataSizeMovedGBInLatestIteration();
    Assertions.assertTrue(size > 0);
    Assertions.assertFalse(size > 10);
    stopBalancer();
  }

  @Test
  public void targetDatanodeShouldNotAlreadyContainSelectedContainer()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(1000);

    stopBalancer();
    Map<ContainerID, DatanodeDetails> map =
        containerBalancer.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry : map.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails target = entry.getValue();
      Assertions.assertTrue(cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .noneMatch(target::equals));
    }
  }

  @Test
  public void containerMoveSelectionShouldFollowPlacementPolicy()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    stopBalancer();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        containerBalancer.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        containerBalancer.getContainerToTargetMap();

    // for each move selection, check if {replicas - source + target}
    // satisfies placement policy
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      ContainerID container = entry.getKey();
      DatanodeDetails source = entry.getValue();

      List<DatanodeDetails> replicas = cidToReplicasMap.get(container)
          .stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      // remove source and add target
      replicas.remove(source);
      replicas.add(containerToTargetMap.get(container));

      ContainerInfo containerInfo = cidToInfoMap.get(container);
      ContainerPlacementStatus placementStatus =
          placementPolicy.validateContainerPlacement(replicas,
              containerInfo.getReplicationConfig().getRequiredNodes());
      Assertions.assertTrue(placementStatus.isPolicySatisfied());
    }
  }

  @Test
  public void targetDatanodeShouldBeInServiceHealthy()
      throws NodeNotFoundException, IllegalContainerBalancerStateException,
      IOException, InvalidContainerBalancerConfigurationException,
      TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    stopBalancer();
    for (DatanodeDetails target : containerBalancer.getSelectedTargets()) {
      NodeStatus status = mockNodeManager.getNodeStatus(target);
      Assertions.assertSame(HddsProtos.NodeOperationalState.IN_SERVICE,
          status.getOperationalState());
      Assertions.assertTrue(status.isHealthy());
    }
  }

  @Test
  public void selectedContainerShouldNotAlreadyHaveBeenSelected()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, NodeNotFoundException,
      TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    balancerConfiguration.setIterations(1);
    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    stopBalancer();

    int numContainers = containerBalancer.getContainerToTargetMap().size();

    /*
    Assuming move is called exactly once for each unique container, number of
     calls to move should equal number of unique containers. If number of
     calls to move is more than number of unique containers, at least one
     container has been re-selected. It's expected that number of calls to
     move should equal number of unique, selected containers (from
     containerToTargetMap).
     */
    Mockito.verify(replicationManager, times(numContainers))
        .move(any(ContainerID.class), any(DatanodeDetails.class),
            any(DatanodeDetails.class));
  }

  @Test
  public void balancerShouldNotSelectConfiguredExcludeContainers()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    balancerConfiguration.setExcludeContainers("1, 4, 5");

    startBalancer(balancerConfiguration);

    sleepWhileBalancing(500);

    stopBalancer();
    Set<ContainerID> excludeContainers =
        balancerConfiguration.getExcludeContainers();
    for (ContainerID container :
        containerBalancer.getContainerToSourceMap().keySet()) {
      Assertions.assertFalse(excludeContainers.contains(container));
    }
  }

  @Test
  public void balancerShouldObeyMaxSizeEnteringTargetLimit()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    conf.set("ozone.scm.container.size", "1MB");
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);

    // no containers should be selected when the limit is just 2 MB
    balancerConfiguration.setMaxSizeEnteringTarget(2 * OzoneConsts.MB);
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);

    Assertions.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    Assertions.assertTrue(containerBalancer.getContainerToSourceMap()
        .isEmpty());
    stopBalancer();

    // some containers should be selected when using default values
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ContainerBalancerConfiguration cbc = ozoneConfiguration.
        getObject(ContainerBalancerConfiguration.class);
    startBalancer(cbc);

    sleepWhileBalancing(500);

    stopBalancer();
    // balancer should have identified unbalanced nodes
    Assertions.assertFalse(containerBalancer.getUnBalancedNodes().isEmpty());
    Assertions.assertFalse(containerBalancer.getContainerToSourceMap()
        .isEmpty());
  }

  @Test
  public void testMetrics()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    conf.set("hdds.datanode.du.refresh.period", "1ms");
    balancerConfiguration.setBalancingInterval(Duration.ofMillis(2));
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(6 * STORAGE_UNIT);
    // deliberately set max size per iteration to a low value, 6 GB
    balancerConfiguration.setMaxSizeToMovePerIteration(6 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);
    stopBalancer();

    ContainerBalancerMetrics metrics = containerBalancer.getMetrics();
    Assertions.assertEquals(determineExpectedUnBalancedNodes(
            balancerConfiguration.getThreshold()).size(),
        metrics.getNumDatanodesUnbalanced());
    Assertions.assertTrue(metrics.getDataSizeMovedGBInLatestIteration() <= 6);
    Assertions.assertEquals(1, metrics.getNumIterations());
  }

  /**
   * Tests if {@link ContainerBalancer} follows the includeNodes and
   * excludeNodes configurations in {@link ContainerBalancerConfiguration}.
   * If the includeNodes configuration is not empty, only the specified
   * includeNodes should be included in balancing. excludeNodes should be
   * excluded from balancing. If a datanode is specified in both include and
   * exclude configurations, then it should be excluded.
   */
  @Test
  public void balancerShouldFollowExcludeAndIncludeDatanodesConfigurations()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    // only these nodes should be included
    // the ones also specified in excludeNodes should be excluded
    int firstIncludeIndex = 0, secondIncludeIndex = 1;
    int thirdIncludeIndex = nodesInCluster.size() - 2;
    int fourthIncludeIndex = nodesInCluster.size() - 1;
    String includeNodes =
        nodesInCluster.get(firstIncludeIndex).getDatanodeDetails()
            .getIpAddress() + ", " +
            nodesInCluster.get(secondIncludeIndex).getDatanodeDetails()
                .getIpAddress() + ", " +
            nodesInCluster.get(thirdIncludeIndex).getDatanodeDetails()
                .getHostName() + ", " +
            nodesInCluster.get(fourthIncludeIndex).getDatanodeDetails()
                .getHostName();

    // these nodes should be excluded
    int firstExcludeIndex = 0, secondExcludeIndex = nodesInCluster.size() - 1;
    String excludeNodes =
        nodesInCluster.get(firstExcludeIndex).getDatanodeDetails()
            .getIpAddress() + ", " +
            nodesInCluster.get(secondExcludeIndex).getDatanodeDetails()
                .getHostName();

    balancerConfiguration.setExcludeNodes(excludeNodes);
    balancerConfiguration.setIncludeNodes(includeNodes);
    startBalancer(balancerConfiguration);
    sleepWhileBalancing(500);
    stopBalancer();

    // finally, these should be the only nodes included in balancing
    // (included - excluded)
    DatanodeDetails dn1 =
        nodesInCluster.get(secondIncludeIndex).getDatanodeDetails();
    DatanodeDetails dn2 =
        nodesInCluster.get(thirdIncludeIndex).getDatanodeDetails();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        containerBalancer.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        containerBalancer.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      DatanodeDetails source = entry.getValue();
      DatanodeDetails target = containerToTargetMap.get(entry.getKey());
      Assertions.assertTrue(source.equals(dn1) || source.equals(dn2));
      Assertions.assertTrue(target.equals(dn1) || target.equals(dn2));
    }
  }

  @Test
  public void testContainerBalancerConfiguration() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.scm.container.size", "5GB");
    ozoneConfiguration.setDouble(
        "hdds.container.balancer.utilization.threshold", 1);

    ContainerBalancerConfiguration cbConf =
        ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
    Assertions.assertEquals(1, cbConf.getThreshold(), 0.001);

    Assertions.assertEquals(26 * 1024 * 1024 * 1024L,
        cbConf.getMaxSizeLeavingSource());

    Assertions.assertEquals(30 * 60 * 1000,
        cbConf.getMoveTimeout().toMillis());
  }

  @Test
  public void checkIterationResult()
      throws NodeNotFoundException, IOException,
      IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException,
      TimeoutException {
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

    startBalancer(balancerConfiguration);
    sleepWhileBalancing(1000);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancer.IterationResult.ITERATION_COMPLETED,
        containerBalancer.getIterationResult());
    stopBalancer();

    /*
    Now, limit maxSizeToMovePerIteration but fail all container moves. The
    result should still be ITERATION_COMPLETED.
     */
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY));
    balancerConfiguration.setMaxSizeToMovePerIteration(10 * STORAGE_UNIT);

    startBalancer(balancerConfiguration);
    sleepWhileBalancing(1000);

    Assertions.assertEquals(
        ContainerBalancer.IterationResult.ITERATION_COMPLETED,
        containerBalancer.getIterationResult());
    stopBalancer();
  }

  @Test
  public void checkIterationResultTimeout()
      throws NodeNotFoundException, IOException,
      IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException,
      TimeoutException {

    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(500), genCompletableFuture(2000));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(1000));

    startBalancer(balancerConfiguration);
    sleepWhileBalancing(2000);

    /*
    According to the setup and configurations, this iteration's result should
    be ITERATION_COMPLETED.
     */
    Assertions.assertEquals(
        ContainerBalancer.IterationResult.ITERATION_COMPLETED,
        containerBalancer.getIterationResult());
    Assertions.assertEquals(1,
        containerBalancer.getMetrics()
            .getNumContainerMovesCompletedInLatestIteration());
    Assertions.assertTrue(containerBalancer.getMetrics()
            .getNumContainerMovesTimeoutInLatestIteration() > 1);
    stopBalancer();

  }

  @Test
  public void checkIterationResultTimeoutFromReplicationManager()
      throws NodeNotFoundException, IOException,
      IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    CompletableFuture<MoveResult> future
        = CompletableFuture.supplyAsync(() ->
        MoveResult.REPLICATION_FAIL_TIME_OUT);
    CompletableFuture<MoveResult> future2
        = CompletableFuture.supplyAsync(() ->
        MoveResult.DELETION_FAIL_TIME_OUT);
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(future, future2);

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(1000));

    startBalancer(balancerConfiguration);
    sleepWhileBalancing(2000);

    Assertions.assertTrue(containerBalancer.getMetrics()
        .getNumContainerMovesTimeoutInLatestIteration() > 0);
    stopBalancer();
  }
  
  @Test
  public void testStartAndImmediateStopForDeadlock()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    startBalancer(balancerConfiguration);
    stopBalancer();
    Assertions.assertFalse(containerBalancer.isBalancerRunning());
  }

  /**
   * Determines unBalanced nodes, that is, over and under utilized nodes,
   * according to the generated utilization values for nodes and the threshold.
   *
   * @param threshold A percentage in the range 0 to 100
   * @return List of DatanodeUsageInfo containing the expected(correct)
   * unBalanced nodes.
   */
  private List<DatanodeUsageInfo> determineExpectedUnBalancedNodes(
      double threshold) {
    threshold /= 100;
    double lowerLimit = averageUtilization - threshold;
    double upperLimit = averageUtilization + threshold;

    // use node utilizations to determine over and under utilized nodes
    List<DatanodeUsageInfo> expectedUnBalancedNodes = new ArrayList<>();
    for (int i = 0; i < numberOfNodes; i++) {
      if (nodeUtilizations.get(numberOfNodes - i - 1) > upperLimit) {
        expectedUnBalancedNodes.add(nodesInCluster.get(numberOfNodes - i - 1));
      }
    }
    for (int i = 0; i < numberOfNodes; i++) {
      if (nodeUtilizations.get(i) < lowerLimit) {
        expectedUnBalancedNodes.add(nodesInCluster.get(i));
      }
    }
    return expectedUnBalancedNodes;
  }

  /**
   * Generates a range of equally spaced utilization(that is, used / capacity)
   * values from 0 to 1.
   *
   * @param count Number of values to generate. Count must be greater than or
   *             equal to 1.
   * @throws IllegalArgumentException If the value of the parameter count is
   * less than 1.
   */
  private void generateUtilizations(int count) throws IllegalArgumentException {
    if (count < 1) {
      LOG.warn("The value of argument count is {}. However, count must be " +
          "greater than 0.", count);
      throw new IllegalArgumentException();
    }
    nodeUtilizations = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      nodeUtilizations.add(i / (double) count);
    }
  }

  /**
   * Create an unbalanced cluster by generating some data. Nodes in the
   * cluster have utilization values determined by generateUtilizations method.
   * @return average utilization (used space / capacity) of the cluster
   */
  private double createCluster() {
    generateData();
    createReplicasForContainers();
    long clusterCapacity = 0, clusterUsedSpace = 0;

    // for each node utilization, calculate that datanode's used space and
    // capacity
    for (int i = 0; i < nodeUtilizations.size(); i++) {
      long datanodeUsedSpace = 0, datanodeCapacity = 0;
      Set<ContainerID> containerIDSet =
          datanodeToContainersMap.get(nodesInCluster.get(i));

      for (ContainerID containerID : containerIDSet) {
        datanodeUsedSpace += cidToInfoMap.get(containerID).getUsedBytes();
      }

      // use node utilization and used space to determine node capacity
      if (nodeUtilizations.get(i) == 0) {
        datanodeCapacity = STORAGE_UNIT * RANDOM.nextInt(10, 60);
      } else {
        datanodeCapacity = (long) (datanodeUsedSpace / nodeUtilizations.get(i));
      }
      SCMNodeStat stat = new SCMNodeStat(datanodeCapacity, datanodeUsedSpace,
          datanodeCapacity - datanodeUsedSpace);
      nodesInCluster.get(i).setScmNodeStat(stat);
      clusterUsedSpace += datanodeUsedSpace;
      clusterCapacity += datanodeCapacity;
    }
    return (double) clusterUsedSpace / clusterCapacity;
  }

  /**
   * Create some datanodes and containers for each node.
   */
  private void generateData() {
    this.numberOfNodes = 10;
    generateUtilizations(numberOfNodes);
    nodesInCluster = new ArrayList<>(nodeUtilizations.size());

    // create datanodes and add containers to them
    for (int i = 0; i < numberOfNodes; i++) {
      Set<ContainerID> containerIDSet = new HashSet<>();
      DatanodeUsageInfo usageInfo =
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              new SCMNodeStat());

      // create containers with varying used space
      int sizeMultiple = 0;
      for (int j = 0; j < i; j++) {
        sizeMultiple %= 5;
        sizeMultiple++;
        ContainerInfo container =
            createContainer((long) i * i + j, sizeMultiple);

        cidToInfoMap.put(container.containerID(), container);
        containerIDSet.add(container.containerID());

        // create initial replica for this container and add it
        Set<ContainerReplica> containerReplicaSet = new HashSet<>();
        containerReplicaSet.add(createReplica(container.containerID(),
            usageInfo.getDatanodeDetails(), container.getUsedBytes()));
        cidToReplicasMap.put(container.containerID(), containerReplicaSet);
      }
      nodesInCluster.add(usageInfo);
      datanodeToContainersMap.put(usageInfo, containerIDSet);
    }
  }

  private ContainerInfo createContainer(long id, int multiple) {
    return new ContainerInfo.Builder()
        .setContainerID(id)
        .setReplicationConfig(RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestContainerBalancer")
        .setUsedBytes(STORAGE_UNIT * multiple)
        .build();
  }

  /**
   * Create the required number of replicas for each container. Note that one
   * replica already exists and nodes with utilization value 0 should not
   * have any replicas.
   */
  private void createReplicasForContainers() {
    for (ContainerInfo container : cidToInfoMap.values()) {

      // one replica already exists; create the remaining ones
      for (int i = 0;
           i < container.getReplicationConfig().getRequiredNodes() - 1; i++) {

        // randomly pick a datanode for this replica
        int datanodeIndex = RANDOM.nextInt(0, numberOfNodes);
        if (nodeUtilizations.get(i) != 0.0d) {
          DatanodeDetails node =
              nodesInCluster.get(datanodeIndex).getDatanodeDetails();
          Set<ContainerReplica> replicas =
              cidToReplicasMap.get(container.containerID());
          replicas.add(createReplica(container.containerID(), node,
              container.getUsedBytes()));
          cidToReplicasMap.put(container.containerID(), replicas);
        }
      }
    }
  }

  private ContainerReplica createReplica(ContainerID containerID,
                                         DatanodeDetails datanodeDetails,
                                         long usedBytes) {
    return ContainerReplica.newBuilder()
        .setContainerID(containerID)
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(datanodeDetails)
        .setOriginNodeId(datanodeDetails.getUuid())
        .setSequenceId(1000L)
        .setBytesUsed(usedBytes)
        .build();
  }

  private void sleepWhileBalancing(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  private void startBalancer(ContainerBalancerConfiguration config)
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    containerBalancer.startBalancer(config);
  }

  private void stopBalancer() {
    try {
      if (containerBalancer.isBalancerRunning()) {
        containerBalancer.stopBalancer();
      }
    } catch (IOException | IllegalContainerBalancerStateException |
             TimeoutException e) {
      LOG.warn("Failed to stop balancer", e);
    }
  }

  private CompletableFuture<LegacyReplicationManager.MoveResult>
      genCompletableFuture(int sleepMilSec) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(sleepMilSec);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return LegacyReplicationManager.MoveResult.COMPLETED;
    });
  }

}
