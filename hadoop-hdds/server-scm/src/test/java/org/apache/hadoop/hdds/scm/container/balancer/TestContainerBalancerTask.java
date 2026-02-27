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

package org.apache.hadoop.hdds.scm.container.balancer;

import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancerTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancerTask.class);

  private MoveManager moveManager;
  private ContainerBalancerTask containerBalancerTask;
  private StorageContainerManager scm;
  private OzoneConfiguration conf;
  private ContainerBalancerConfiguration balancerConfiguration;
  private List<DatanodeUsageInfo> nodesInCluster;
  private List<Double> nodeUtilizations;
  private int numberOfNodes;
  private Map<ContainerID, Set<ContainerReplica>> cidToReplicasMap =
      new HashMap<>();
  private Map<ContainerID, ContainerInfo> cidToInfoMap = new HashMap<>();
  private Map<DatanodeUsageInfo, Set<ContainerID>> datanodeToContainersMap =
      new HashMap<>();
  private Map<String, ByteString> serviceToConfigMap = new HashMap<>();
  private static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();

  static final long STORAGE_UNIT = OzoneConsts.GB;

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException, NodeNotFoundException,
      TimeoutException {
    conf = new OzoneConfiguration();
    ReplicationManagerConfiguration rmConf = new ReplicationManagerConfiguration();
    scm = mock(StorageContainerManager.class);
    ContainerManager containerManager = mock(ContainerManager.class);
    ReplicationManager replicationManager = mock(ReplicationManager.class);
    StatefulServiceStateManager serviceStateManager = mock(StatefulServiceStateManagerImpl.class);
    SCMServiceManager scmServiceManager = mock(SCMServiceManager.class);
    moveManager = mock(MoveManager.class);
    when(moveManager.move(any(ContainerID.class),
            any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.COMPLETED));

    when(replicationManager.getConfig()).thenReturn(rmConf);
    // these configs will usually be specified in each test
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    balancerConfiguration.setMaxSizeToMovePerIteration(50 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(50 * STORAGE_UNIT);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancerTask.class, Level.DEBUG);

    int[] sizeArray = testInfo.getTestMethod()
            .filter(method -> method.getName().equals("balancerShouldMoveOnlyPositiveSizeContainers"))
            .map(method -> new int[]{0, 0, 0, 0, 0, 1, 2, 3, 4, 5})
            .orElse(null);
    createCluster(sizeArray);
    MockNodeManager mockNodeManager = new MockNodeManager(datanodeToContainersMap);

    NetworkTopology clusterMap = mockNodeManager.getClusterNetworkTopologyMap();

    PlacementPolicy placementPolicy = ContainerPlacementPolicyFactory
                                          .getPolicy(conf, mockNodeManager, clusterMap, true,
                                              SCMContainerPlacementMetrics.create());
    PlacementPolicy ecPlacementPolicy = ContainerPlacementPolicyFactory.getECPolicy(
        conf, mockNodeManager, clusterMap,
        true, SCMContainerPlacementMetrics.create());
    PlacementPolicyValidateProxy placementPolicyValidateProxy = new PlacementPolicyValidateProxy(
        placementPolicy, ecPlacementPolicy);

    when(replicationManager
        .isContainerReplicatingOrDeleting(any(ContainerID.class)))
        .thenReturn(false);

    when(replicationManager.getClock())
        .thenReturn(Clock.system(ZoneId.systemDefault()));

    when(containerManager.getContainerReplicas(any(ContainerID.class)))
        .thenAnswer(invocationOnMock -> {
          ContainerID cid = (ContainerID) invocationOnMock.getArguments()[0];
          return cidToReplicasMap.get(cid);
        });

    when(containerManager.getContainer(any(ContainerID.class)))
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
    when(scm.getMoveManager()).thenReturn(moveManager);

    /*
    When StatefulServiceStateManager#saveConfiguration is called, save to
    in-memory serviceToConfigMap instead.
     */
    doAnswer(i -> {
      serviceToConfigMap.put(i.getArgument(0, String.class), i.getArgument(1,
          ByteString.class));
      return null;
    }).when(serviceStateManager).saveConfiguration(
        any(String.class),
        any(ByteString.class));

    /*
    When StatefulServiceStateManager#readConfiguration is called, read from
    serviceToConfigMap instead.
     */
    when(serviceStateManager.readConfiguration(anyString())).thenAnswer(
        i -> serviceToConfigMap.get(i.getArgument(0, String.class)));

    doNothing().when(scmServiceManager)
        .register(any(SCMService.class));
    ContainerBalancer sb = new ContainerBalancer(scm);
    containerBalancerTask = new ContainerBalancerTask(scm, 0, sb,
        sb.getMetrics(), balancerConfiguration, false);
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
    stopBalancer();

    // finally, these should be the only nodes included in balancing
    // (included - excluded)
    DatanodeDetails dn1 =
        nodesInCluster.get(secondIncludeIndex).getDatanodeDetails();
    DatanodeDetails dn2 =
        nodesInCluster.get(thirdIncludeIndex).getDatanodeDetails();
    Map<ContainerID, DatanodeDetails> containerFromSourceMap =
        containerBalancerTask.getContainerToSourceMap();
    Map<ContainerID, DatanodeDetails> containerToTargetMap =
        containerBalancerTask.getContainerToTargetMap();
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
        containerFromSourceMap.entrySet()) {
      DatanodeDetails source = entry.getValue();
      DatanodeDetails target = containerToTargetMap.get(entry.getKey());
      assertTrue(source.equals(dn1) || source.equals(dn2));
      assertTrue(target.equals(dn1) || target.equals(dn2));
    }
  }

  @Test
  public void testContainerBalancerConfiguration() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("ozone.scm.container.size", "5GB");
    ozoneConfiguration.setDouble(
        "hdds.container.balancer.utilization.threshold", 1);
    long maxSizeLeavingSource = 26;
    ozoneConfiguration.setStorageSize(
        "hdds.container.balancer.size.leaving.source.max", maxSizeLeavingSource,
        StorageUnit.GB);
    long moveTimeout = 90;
    ozoneConfiguration.setTimeDuration("hdds.container.balancer.move.timeout",
        moveTimeout, TimeUnit.MINUTES);
    long replicationTimeout = 60;
    ozoneConfiguration.setTimeDuration(
        "hdds.container.balancer.move.replication.timeout",
        replicationTimeout, TimeUnit.MINUTES);

    ContainerBalancerConfiguration cbConf =
        ozoneConfiguration.getObject(ContainerBalancerConfiguration.class);
    assertEquals(1, cbConf.getThreshold(), 0.001);

    // Expected is 26 GB
    assertEquals(maxSizeLeavingSource * 1024 * 1024 * 1024,
        cbConf.getMaxSizeLeavingSource());
    assertEquals(moveTimeout, cbConf.getMoveTimeout().toMinutes());
    assertEquals(replicationTimeout,
        cbConf.getMoveReplicationTimeout().toMinutes());
  }

  @Test
  public void testDelayedStart() throws InterruptedException, TimeoutException {
    conf.setTimeDuration("hdds.scm.wait.time.after.safemode.exit", 10,
        TimeUnit.SECONDS);
    ContainerBalancer balancer = new ContainerBalancer(scm);
    containerBalancerTask = new ContainerBalancerTask(scm, 2, balancer,
        balancer.getMetrics(), balancerConfiguration, true);
    Thread balancingThread = new Thread(containerBalancerTask);
    // start the thread and assert that balancer is RUNNING
    balancingThread.start();
    assertEquals(ContainerBalancerTask.Status.RUNNING, containerBalancerTask.getBalancerStatus());

    /*
     Wait for the thread to start sleeping and assert that it's sleeping.
     This is the delay before it starts balancing.
     */
    GenericTestUtils.waitFor(
        () -> balancingThread.getState() == Thread.State.TIMED_WAITING, 1, 40);
    assertEquals(Thread.State.TIMED_WAITING,
        balancingThread.getState());

    // interrupt the thread from its sleep, wait and assert that balancer has
    // STOPPED
    balancingThread.interrupt();
    GenericTestUtils.waitFor(() -> containerBalancerTask.getBalancerStatus() ==
        ContainerBalancerTask.Status.STOPPED, 1, 20);
    assertEquals(ContainerBalancerTask.Status.STOPPED, containerBalancerTask.getBalancerStatus());

    // ensure the thread dies
    GenericTestUtils.waitFor(() -> !balancingThread.isAlive(), 1, 20);
    assertFalse(balancingThread.isAlive());
  }

  /**
   * Tests if balancer is adding the polled source datanode back to potentialSources queue
   * if a move has failed due to a container related failure, like REPLICATION_FAIL_NOT_EXIST_IN_SOURCE.
   */
  @Test
  public void testSourceDatanodeAddedBack()
      throws NodeNotFoundException, IOException, IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException, TimeoutException, InterruptedException {

    when(moveManager.move(any(ContainerID.class),
        any(DatanodeDetails.class),
        any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(1);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    String includeNodes = nodesInCluster.get(0).getDatanodeDetails().getHostName() + "," +
        nodesInCluster.get(nodesInCluster.size() - 1).getDatanodeDetails().getHostName();
    balancerConfiguration.setIncludeNodes(includeNodes);

    startBalancer(balancerConfiguration);
    GenericTestUtils.waitFor(() -> ContainerBalancerTask.IterationResult.ITERATION_COMPLETED ==
        containerBalancerTask.getIterationResult(), 10, 50);

    assertEquals(2, containerBalancerTask.getCountDatanodesInvolvedPerIteration());
    assertTrue(containerBalancerTask.getMetrics().getNumContainerMovesCompletedInLatestIteration() >= 1);
    assertThat(containerBalancerTask.getMetrics().getNumContainerMovesFailed()).isEqualTo(1);
    assertTrue(containerBalancerTask.getSelectedTargets().contains(nodesInCluster.get(0)
        .getDatanodeDetails()));
    assertTrue(containerBalancerTask.getSelectedSources().contains(nodesInCluster.get(nodesInCluster.size() - 1)
        .getDatanodeDetails()));
    stopBalancer();
  }

   /**
   * Test to check if balancer picks up only positive size
   * containers to move from source to destination.
   */
  @Test
  public void balancerShouldMoveOnlyPositiveSizeContainers()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {

    startBalancer(balancerConfiguration);
    /*
     Get all containers that were selected by balancer and assert none of
     them is a zero or negative size container.
     */
    Map<ContainerID, DatanodeDetails> containerToSource =
            containerBalancerTask.getContainerToSourceMap();
    assertFalse(containerToSource.isEmpty());
    boolean zeroOrNegSizeContainerMoved = false;
    for (Map.Entry<ContainerID, DatanodeDetails> entry :
            containerToSource.entrySet()) {
      ContainerInfo containerInfo = cidToInfoMap.get(entry.getKey());
      if (containerInfo.getUsedBytes() <= 0) {
        zeroOrNegSizeContainerMoved = true;
      }
    }
    assertFalse(zeroOrNegSizeContainerMoved);
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
  private void createCluster(int[] sizeArray) {
    generateData(sizeArray);
    createReplicasForContainers();

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
          datanodeCapacity - datanodeUsedSpace, 0,
          datanodeCapacity - datanodeUsedSpace - 1, 0);
      nodesInCluster.get(i).setScmNodeStat(stat);
    }
  }

  /**
   * Create some datanodes and containers for each node.
   */
  private void generateData(int[] sizeArray) {
    this.numberOfNodes = 10;
    generateUtilizations(numberOfNodes);
    nodesInCluster = new ArrayList<>(nodeUtilizations.size());

    // create datanodes and add containers to them
    for (int i = 0; i < numberOfNodes; i++) {
      Set<ContainerID> containerIDSet = new HashSet<>();
      DatanodeUsageInfo usageInfo =
          new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(),
              new SCMNodeStat());

      int sizeMultiple = 0;
      if (sizeArray == null) {
        sizeArray = new int[10];
        for (int j = 0; j < numberOfNodes; j++) {
          sizeArray[j] = sizeMultiple;
          sizeMultiple %= 5;
          sizeMultiple++;
        }
      }
      // create containers with varying used space
      for (int j = 0; j < i; j++) {
        ContainerInfo container =
            createContainer((long) i * i + j, sizeArray[j]);

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
    ContainerInfo.Builder builder = new ContainerInfo.Builder()
        .setContainerID(id)
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setOwner("TestContainerBalancer")
        .setUsedBytes(STORAGE_UNIT * multiple);

    /*
    Make it a RATIS container if id is even, else make it an EC container
     */
    if (id % 2 == 0) {
      builder.setReplicationConfig(RatisReplicationConfig
          .getInstance(HddsProtos.ReplicationFactor.THREE));
    } else {
      builder.setReplicationConfig(new ECReplicationConfig(3, 2));
    }

    return builder.build();
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
        // don't put replicas in DNs that are supposed to have 0 utilization
        if (Math.abs(nodeUtilizations.get(datanodeIndex) - 0.0d) > 0.00001) {
          DatanodeDetails node =
              nodesInCluster.get(datanodeIndex).getDatanodeDetails();
          Set<ContainerReplica> replicas =
              cidToReplicasMap.get(container.containerID());
          replicas.add(createReplica(container.containerID(), node,
              container.getUsedBytes()));
          cidToReplicasMap.put(container.containerID(), replicas);
          datanodeToContainersMap.get(nodesInCluster.get(datanodeIndex))
              .add(container.containerID());
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
        .setOriginNodeId(datanodeDetails.getID())
        .setSequenceId(1000L)
        .setBytesUsed(usedBytes)
        .build();
  }

  private void startBalancer(ContainerBalancerConfiguration config)
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    containerBalancerTask.setConfig(config);
    containerBalancerTask.setTaskStatus(ContainerBalancerTask.Status.RUNNING);
    containerBalancerTask.run();
  }

  private void stopBalancer() {
    // do nothing as testcase is not threaded
  }
}
