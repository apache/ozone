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
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager.MoveResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
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

import static org.mockito.Mockito.mock;
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
  public void testShouldRun() throws Exception {
    boolean doRun = containerBalancer.shouldRun();
    // should be equal to average utilization of the cluster
    Assertions.assertEquals(doRun, false);
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertEquals(doRun, true);
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertEquals(doRun, false);
  }

  @Test
  public void testStartBalancerStop() throws Exception {
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(1000));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(1000));

    startBalancer(balancerConfiguration);
    try {
      containerBalancer.startBalancer(balancerConfiguration);
      Assertions.assertTrue(false);
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    try {
      containerBalancer.start();
      Assertions.assertTrue(false);
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    stopBalancer();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);

    try {
      containerBalancer.stopBalancer();
      Assertions.assertTrue(false);
    } catch (Exception e) {
      // stop failed as already stopped, valid case
    }
  }

  @Test
  public void testStartStop() throws Exception {
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(1000));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(1000));
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    containerBalancer.start();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    try {
      containerBalancer.start();
    } catch (IllegalContainerBalancerStateException e) {
      // start failed
      Assertions.assertTrue(true);
    }

    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
  }

  @Test
  public void testNotifyStateChangeStopStart() throws Exception {
    Mockito.when(replicationManager.move(Mockito.any(ContainerID.class),
            Mockito.any(DatanodeDetails.class),
            Mockito.any(DatanodeDetails.class)))
        .thenReturn(genCompletableFuture(1000));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMoveTimeout(Duration.ofMillis(1000));

    containerBalancer.startBalancer(balancerConfiguration);

    scm.getScmContext().updateLeaderAndTerm(false, 1);
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
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
