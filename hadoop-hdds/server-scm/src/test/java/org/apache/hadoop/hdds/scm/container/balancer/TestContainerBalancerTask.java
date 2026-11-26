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

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStorageTypeStatusInfoProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.PlacementPolicyValidateProxy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicyFactory;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementMetrics;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.container.replication.ContainerHealthResult;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
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
            any(DatanodeDetails.class), any(DatanodeDetails.class), any(StorageType.class)))
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
    when(replicationManager.getContainerReplicationHealth(any(ContainerInfo.class), anySet()))
        .thenAnswer(invocationOnMock -> new ContainerHealthResult.HealthyResult(invocationOnMock.getArgument(0)));

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
   * Tests that pre-move failures return the source to the queue. Post-move health failures also exclude the selected
   * container for this iteration. A changed storage type excludes it only for the storage type that was moved.
   */
  @ParameterizedTest
  @EnumSource(value = MoveManager.MoveResult.class,
      names = {"REPLICATION_FAIL_NOT_EXIST_IN_SOURCE", "REPLICATION_NOT_HEALTHY_BEFORE_MOVE", 
          "FAIL_CONTAINER_ALREADY_BEING_MOVED", "REPLICATION_NOT_HEALTHY_AFTER_MOVE",
          "REPLICATION_FAIL_STORAGE_TYPE_MISMATCH"})
  public void testSourceDatanodeAddedBack(MoveManager.MoveResult moveResult)
      throws Exception {
    when(moveManager.move(any(ContainerID.class), any(DatanodeDetails.class),
        any(DatanodeDetails.class), any(StorageType.class)))
        .thenReturn(CompletableFuture.completedFuture(moveResult))
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
    assertTrue(containerBalancerTask.getSelectedSources().contains(
        nodesInCluster.get(nodesInCluster.size() - 1).getDatanodeDetails()));

    ArgumentCaptor<ContainerID> containerCaptor = ArgumentCaptor.forClass(ContainerID.class);
    verify(moveManager, atLeast(1)).move(containerCaptor.capture(),
        any(DatanodeDetails.class), any(DatanodeDetails.class), any(StorageType.class));
    ContainerID failedContainerId = containerCaptor.getAllValues().get(0);
    ContainerBalancerSelectionCriteria criteria = containerBalancerTask.getSelectionCriteria();
    assertEquals(moveResult == MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE,
        criteria.getExcludeDueToFailContainers().contains(failedContainerId));
    assertEquals(moveResult == MoveManager.MoveResult.REPLICATION_FAIL_STORAGE_TYPE_MISMATCH,
        criteria.getExcludeDueToStorageTypeMismatch(StorageType.DISK).contains(failedContainerId));
    assertThat(criteria.getExcludeDueToStorageTypeMismatch(StorageType.SSD)).doesNotContain(failedContainerId);
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

  @Test
  public void testConcurrentMoveCallbacksAccumulateMovedBytesAtomically() throws Exception {
    int concurrentParties = 10;
    CyclicBarrier completionBarrier = new CyclicBarrier(concurrentParties);
    AtomicInteger completionOrder = new AtomicInteger(0);
    ExecutorService moveCompletionExecutor = Executors.newFixedThreadPool(concurrentParties);

    try {
      when(moveManager.move(any(ContainerID.class), any(DatanodeDetails.class),
          any(DatanodeDetails.class), any(StorageType.class)))
          .thenAnswer(invocation -> {
            CompletableFuture<MoveManager.MoveResult> future = new CompletableFuture<>();
            int order = completionOrder.getAndIncrement();

            moveCompletionExecutor.execute(() -> {
              try {
                //This forces 10 completion threads to release together
                if (order < concurrentParties) {
                  completionBarrier.await(30, TimeUnit.SECONDS);
                }
              } catch (Exception e) {
                future.completeExceptionally(e);
                return;
              }
              future.complete(MoveManager.MoveResult.COMPLETED);
            });
            return future;
          });

      balancerConfiguration.setThreshold(10);
      balancerConfiguration.setIterations(1);
      balancerConfiguration.setMaxSizeEnteringTarget(500 * STORAGE_UNIT);
      balancerConfiguration.setMaxSizeToMovePerIteration(500 * STORAGE_UNIT);
      balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);

      startBalancer(balancerConfiguration);

      ContainerBalancerMetrics metrics = containerBalancerTask.getMetrics();
      int completedMoves = (int) metrics.getNumContainerMovesCompletedInLatestIteration();

      assertTrue(completedMoves >= concurrentParties, 
          "Expected at least " + concurrentParties + " completed moves");

      long expectedBytesMoved = 0;
      for (ContainerID containerID : containerBalancerTask.getContainerToSourceMap().keySet()) {
        expectedBytesMoved += cidToInfoMap.get(containerID).getUsedBytes();
      }

      assertEquals(expectedBytesMoved, metrics.getDataSizeMovedInLatestIteration());
    } finally {
      moveCompletionExecutor.shutdownNow();
    }
  }

  @Test
  public void testExcludeContainersNotFoundPersistsAcrossIterations() throws Exception {
    DatanodeUsageInfo sourceNode = nodesInCluster.get(nodesInCluster.size() - 1);
    ContainerManager containerManager = scm.getContainerManager();
    final ContainerID[] missingContainerId = new ContainerID[1];
    final AtomicInteger getContainerCallsInLaterIterations = new AtomicInteger(0);

    // Container exists during candidate selection; after it is first selected for
    // move, SCM can no longer find it (simulates container deleted mid-balance).
    when(moveManager.move(any(ContainerID.class), any(DatanodeDetails.class),
        any(DatanodeDetails.class), any(StorageType.class)))
        .thenAnswer(invocation -> {
          ContainerID containerId = invocation.getArgument(0);
          if (missingContainerId[0] == null) {
            missingContainerId[0] = containerId;
            doAnswer(inv -> {
              if (containerBalancerTask.getMetrics().getNumIterations() >= 1) {
                getContainerCallsInLaterIterations.incrementAndGet();
              }
              throw ContainerNotFoundException.newInstanceForTesting();
            }).when(containerManager).getContainer(containerId);
            doThrow(ContainerNotFoundException.newInstanceForTesting())
                .when(containerManager).getContainerReplicas(containerId);
            cidToInfoMap.remove(containerId);
          }
          return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
        });

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(2);
    balancerConfiguration.setBalancingInterval(0);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    String includeNodes = nodesInCluster.get(0).getDatanodeDetails().getHostName() + "," +
        sourceNode.getDatanodeDetails().getHostName();
    balancerConfiguration.setIncludeNodes(includeNodes);

    startBalancer(balancerConfiguration);

    assertEquals(2, containerBalancerTask.getMetrics().getNumIterations(),
        "Balancer must complete both iterations");

    ContainerID notFoundId = missingContainerId[0];
    assertNotNull(notFoundId, "Expected at least one container move");
    assertTrue(containerBalancerTask.getSelectionCriteria()
            .getExcludeNotFoundContainers().contains(notFoundId),
        "Missing container should be in the cross-iteration exclude set");

    // iteration 2 must not probe the missing container again.
    assertEquals(0, getContainerCallsInLaterIterations.get(),
        "Iteration 2 must not call getContainer for the not-found container");

    ArgumentCaptor<ContainerID> containerCaptor = ArgumentCaptor.forClass(ContainerID.class);
    verify(moveManager, atLeast(1)).move(containerCaptor.capture(),
        any(DatanodeDetails.class), any(DatanodeDetails.class), any(StorageType.class));

    long moveAttempts = containerCaptor.getAllValues().stream()
        .filter(id -> id.equals(notFoundId))
        .count();
    assertEquals(1, moveAttempts,
        "Permanently missing container should not be retried in later iterations");
  }

  @Test
  public void testTransientFailureNotPersistedAcrossIterations() throws Exception {
    when(moveManager.move(any(ContainerID.class), any(DatanodeDetails.class),
        any(DatanodeDetails.class), any(StorageType.class)))
        .thenReturn(CompletableFuture.completedFuture(
            MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));

    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(2);
    balancerConfiguration.setBalancingInterval(0);
    balancerConfiguration.setMaxSizeEnteringTarget(10 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(100);
    String includeNodes = nodesInCluster.get(0).getDatanodeDetails().getHostName() + "," +
        nodesInCluster.get(nodesInCluster.size() - 1).getDatanodeDetails().getHostName();
    balancerConfiguration.setIncludeNodes(includeNodes);

    startBalancer(balancerConfiguration);

    assertEquals(2, containerBalancerTask.getMetrics().getNumIterations(),
        "Balancer must complete both iterations");

    ArgumentCaptor<ContainerID> containerCaptor = ArgumentCaptor.forClass(ContainerID.class);
    verify(moveManager, atLeast(1)).move(containerCaptor.capture(),
        any(DatanodeDetails.class), any(DatanodeDetails.class), any(StorageType.class));
    ContainerID failedContainerId = containerCaptor.getAllValues().get(0);

    // Transient failure must not land in the cross-iteration not-found set.
    assertFalse(containerBalancerTask.getSelectionCriteria()
            .getExcludeNotFoundContainers().contains(failedContainerId),
        "Transient failure must not be treated as permanently missing");

    // Iteration 2 should retry the same container because excludeDueToFailure
    // is iteration-scoped and cleared between iterations.
    long moveAttempts = containerCaptor.getAllValues().stream()
        .filter(id -> id.equals(failedContainerId))
        .count();
    assertEquals(2, moveAttempts,
        "Transient failure should be retried in the next iteration");
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
      SCMNodeStat stat = new SCMNodeStat(
          singletonMap(StorageType.DEFAULT, datanodeCapacity),
          singletonMap(StorageType.DEFAULT, datanodeUsedSpace),
          singletonMap(StorageType.DEFAULT, datanodeCapacity - datanodeUsedSpace),
          singletonMap(StorageType.DEFAULT, 0L),
          singletonMap(StorageType.DEFAULT, datanodeCapacity - datanodeUsedSpace - 1),
          singletonMap(StorageType.DEFAULT, 0L));
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
        .setStorageType(StorageType.DISK)
        .setVolumeStorageType(StorageType.DISK)
        .build();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRejectedCandidatesAreNotRecheckedForStorageType(boolean differentStorageType) throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 80);
    StorageType rejectedType = differentStorageType ? StorageType.DISK : StorageType.SSD;
    List<ContainerID> rejected = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      long id = 1000L + i;
      addStorageTypeContainer(source, id, rejectedType, 2 * OzoneConsts.MB);
      ContainerID containerID = ContainerID.valueOf(id);
      if (!differentStorageType) {
        cidToInfoMap.get(containerID).setState(HddsProtos.LifeCycleState.OPEN);
      }
      rejected.add(containerID);
    }
    for (int i = 0; i < 100; i++) {
      addStorageTypeContainer(source, 2000L + i, StorageType.SSD, OzoneConsts.MB);
    }
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * OzoneConsts.MB);
    startBalancer(balancerConfiguration);

    assertThat(order).hasSize(100).containsOnly(StorageType.SSD);
    for (ContainerID containerID : rejected) {
      verify(scm.getContainerManager(), times(1)).getContainerReplicas(containerID);
    }
  }

  /**
   * A DISK container on the source is seen during the SSD pass and rejected there, either by the SSD utilization limit
   * or by its storage type. That rejection only applies to the SSD pass: candidates are kept per storage type, so the
   * same container is still selected and moved in the DISK pass.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRejectedContainerRemainsAvailableForAnotherStorageType(boolean exceedsSsdLimit) throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 95);
    DatanodeUsageInfo target = storageTypeNode(20, 5);
    addStorageTypeContainer(source, 1000, StorageType.DISK, (exceedsSsdLimit ? 50 : 20) * STORAGE_UNIT);
    addStorageTypeContainer(source, 1001, StorageType.SSD, 10 * STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.SSD, StorageType.DISK);
    assertThat(containerBalancerTask.getContainerToSourceMap()).containsOnlyKeys(
        ContainerID.valueOf(1000), ContainerID.valueOf(1001));
  }

  /**
   * One container has an SSD replica on one source and a DISK replica on another. Each storage type's pass may only use
   * the source whose replica is on that type, and the container is moved at most once per iteration.
   */
  @Test
  public void testMixedStorageTypeContainerMovesOnceFromMatchingSource() throws Exception {
    DatanodeUsageInfo ssdSource = storageTypeNode(90, 50);
    DatanodeUsageInfo diskSource = storageTypeNode(50, 90);
    DatanodeUsageInfo target = storageTypeNode(10, 10);
    Map<DatanodeUsageInfo, StorageType> replicaTypes = new HashMap<>();
    replicaTypes.put(ssdSource, StorageType.SSD);
    replicaTypes.put(diskSource, StorageType.DISK);
    addMixedStorageTypeContainer(1000, 10 * STORAGE_UNIT, replicaTypes);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdSource, diskSource, target), 0, order);
    Map<StorageType, DatanodeDetails> sourceByType = new EnumMap<>(StorageType.class);
    doAnswer(invocation -> {
      sourceByType.put(invocation.getArgument(3), invocation.getArgument(1));
      order.add(invocation.getArgument(3));
      return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    }).when(moveManager).move(any(), any(), any(), any());
    startBalancer(balancerConfiguration);

    assertThat(order).hasSize(1);
    StorageType moved = order.get(0);
    assertEquals(moved == StorageType.SSD ? ssdSource.getDatanodeDetails() : diskSource.getDatanodeDetails(),
        sourceByType.get(moved));
    assertThat(containerBalancerTask.getContainerToSourceMap()).containsOnlyKeys(ContainerID.valueOf(1000));
  }

  /**
   * A storage-type mismatch during the SSD pass excludes the container only for SSD, so the DISK pass can still move
   * the same container from its DISK replica in the same iteration.
   */
  @Test
  public void testStorageTypeMismatchDoesNotBlockOtherStorageType() throws Exception {
    DatanodeUsageInfo ssdSource = storageTypeNode(90, 50);
    DatanodeUsageInfo diskSource = storageTypeNode(50, 90);
    DatanodeUsageInfo target = storageTypeNode(10, 10);
    Map<DatanodeUsageInfo, StorageType> replicaTypes = new HashMap<>();
    replicaTypes.put(ssdSource, StorageType.SSD);
    replicaTypes.put(diskSource, StorageType.DISK);
    addMixedStorageTypeContainer(1000, 10 * STORAGE_UNIT, replicaTypes);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdSource, diskSource, target), 0, order);
    doAnswer(invocation -> {
      StorageType type = invocation.getArgument(3);
      order.add(type);
      return CompletableFuture.completedFuture(type == StorageType.SSD
          ? MoveManager.MoveResult.REPLICATION_FAIL_STORAGE_TYPE_MISMATCH : MoveManager.MoveResult.COMPLETED);
    }).when(moveManager).move(any(), any(), any(), any());
    startBalancer(balancerConfiguration);

    ContainerID id = ContainerID.valueOf(1000);
    assertThat(order).containsExactly(StorageType.SSD, StorageType.DISK);
    assertEquals(diskSource.getDatanodeDetails(), containerBalancerTask.getContainerToSourceMap().get(id));
    ContainerBalancerSelectionCriteria criteria = containerBalancerTask.getSelectionCriteria();
    assertThat(criteria.getExcludeDueToStorageTypeMismatch(StorageType.SSD)).containsExactly(id);
    assertThat(criteria.getExcludeDueToStorageTypeMismatch(StorageType.DISK)).isEmpty();
    assertThat(criteria.getExcludeDueToFailContainers()).doesNotContain(id);
  }

  /**
   * Only the replica on the source decides a typed move. A conflicting or unknown replica on another datanode does not
   * stop the container from moving from a source whose replica matches.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testOtherDatanodeReplicaDoesNotAffectTypedMove(boolean otherConflicting) throws Exception {
    DatanodeUsageInfo source = storageTypeNode(90, 50);
    DatanodeUsageInfo other = storageTypeNode(50, 50);
    DatanodeUsageInfo target = storageTypeNode(10, 50);
    addStorageTypeContainer(source, 1000, StorageType.SSD, 10 * STORAGE_UNIT);
    ContainerID id = ContainerID.valueOf(1000);
    Set<ContainerReplica> replicas = new HashSet<>(cidToReplicasMap.get(id));
    replicas.add(otherConflicting
        ? storageTypeReplica(id, other, StorageType.SSD, StorageType.DISK, 10 * STORAGE_UNIT)
        : storageTypeReplica(id, other, null, null, 10 * STORAGE_UNIT));
    cidToReplicasMap.put(id, replicas);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, other, target), 0, order);
    doAnswer(invocation -> {
      assertEquals(source.getDatanodeDetails(), invocation.getArgument(1));
      order.add(invocation.getArgument(3));
      return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    }).when(moveManager).move(any(), any(), any(), any());
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.SSD);
    assertEquals(source.getDatanodeDetails(), containerBalancerTask.getContainerToSourceMap().get(id));
  }

  @ParameterizedTest
  @CsvSource({"0,true", "1,true", "0,false", "1,false"})
  public void testFirstStorageTypeRotatesWithSmallSharedLimit(int iteration, boolean nodeLimit) throws Exception {
    DatanodeUsageInfo ssdSource = storageTypeNode(80, -1);
    DatanodeUsageInfo ssdTarget = storageTypeNode(20, -1);
    DatanodeUsageInfo diskSource = storageTypeNode(-1, 80);
    DatanodeUsageInfo diskTarget = storageTypeNode(-1, 20);
    addStorageTypeContainer(ssdSource, 1000, StorageType.SSD, STORAGE_UNIT);
    addStorageTypeContainer(diskSource, 1001, StorageType.DISK, STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdSource, ssdTarget, diskSource, diskTarget), iteration, order);
    if (nodeLimit) {
      balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(50);
    } else {
      balancerConfiguration.setMaxSizeToMovePerIteration(STORAGE_UNIT);
    }
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(iteration == 0 ? StorageType.SSD : StorageType.DISK);
    assertEquals(nodeLimit ? 0 : 1, containerBalancerTask.getMetrics().getNumIterationsReachedMaxSizeToMoveLimit());
    assertEquals(2, containerBalancerTask.getMetrics().getNumDatanodesInvolvedInLatestIteration());
  }

  @Test
  public void testTierImbalanceIsNotHiddenByBalancedWholeNodeUsage() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(95, 0);
    DatanodeUsageInfo target = storageTypeNode(20, 75);
    addStorageTypeContainer(source, 1000, StorageType.SSD, 10 * STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.SSD);
    assertEquals(target.getDatanodeDetails(),
        containerBalancerTask.getContainerToTargetMap().get(ContainerID.valueOf(1000)));
    ContainerBalancerClusterSnapshot snapshot = ContainerBalancerClusterAnalyzer.analyze(Arrays.asList(
        source.toProto(ClientVersion.CURRENT), target.toProto(ClientVersion.CURRENT)),
        balancerConfiguration.getThresholdAsRatio(), Collections.emptySet(), Collections.emptySet());
    assertEquals(snapshot.getSourceDatanodes(), containerBalancerTask.getOverUtilizedNodes().stream()
        .map(node -> node.getDatanodeID().toString()).collect(Collectors.toSet()));
    assertEquals(snapshot.getTargetDatanodes(), containerBalancerTask.getUnderUtilizedNodes().stream()
        .map(node -> node.getDatanodeID().toString()).collect(Collectors.toSet()));
  }

  /**
   * A node outside the threshold band is reported as unbalanced even when its storage type has no target, as in
   * aggregate balancing. Only the storage type with both a source and a target produces a move.
   */
  @Test
  public void testOneSidedStorageTypeIsReportedButNotMoved() throws Exception {
    // SSD 95/70/70: average 78.3%, so only 95% is outside the 10% band and SSD has a source but no target.
    // DISK 50/90/10: average 50%, so 90% is a source and 10% is a target.
    DatanodeUsageInfo ssdOnlySource = storageTypeNode(95, 50);
    DatanodeUsageInfo diskSource = storageTypeNode(70, 90);
    DatanodeUsageInfo diskTarget = storageTypeNode(70, 10);
    addStorageTypeContainer(ssdOnlySource, 1000, StorageType.SSD, 10 * STORAGE_UNIT);
    addStorageTypeContainer(diskSource, 1001, StorageType.DISK, 10 * STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdOnlySource, diskSource, diskTarget), 0, order);
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.DISK);
    assertThat(containerBalancerTask.getContainerToSourceMap()).containsOnlyKeys(ContainerID.valueOf(1001));
    assertEquals(new HashSet<>(Arrays.asList(ssdOnlySource.getDatanodeDetails(), diskSource.getDatanodeDetails())),
        containerBalancerTask.getOverUtilizedNodes().stream()
            .map(DatanodeUsageInfo::getDatanodeDetails).collect(Collectors.toSet()));
    assertEquals(Collections.singleton(diskTarget.getDatanodeDetails()),
        containerBalancerTask.getUnderUtilizedNodes().stream()
            .map(DatanodeUsageInfo::getDatanodeDetails).collect(Collectors.toSet()));
    assertEquals(3, containerBalancerTask.getMetrics().getNumDatanodesUnbalanced());
  }

  /**
   * When every storage type has only sources or only targets, the nodes are still reported as unbalanced, no move
   * is made, and the balancer stops because nothing more can be balanced, as in aggregate balancing.
   */
  @Test
  public void testEveryStorageTypeOneSidedMakesNoMove() throws Exception {
    // SSD 95/70/70: average 78.3%, so SSD has a source but no target.
    // DISK 30/10/30: average 23.3%, so 10% is a target and DISK has no source.
    DatanodeUsageInfo ssdSource = storageTypeNode(95, 30);
    DatanodeUsageInfo diskTarget = storageTypeNode(70, 10);
    DatanodeUsageInfo balanced = storageTypeNode(70, 30);
    addStorageTypeContainer(ssdSource, 1000, StorageType.SSD, 10 * STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdSource, diskTarget, balanced), 0, order);
    startBalancer(balancerConfiguration);

    assertThat(order).isEmpty();
    assertThat(containerBalancerTask.getContainerToSourceMap()).isEmpty();
    assertEquals(Collections.singleton(ssdSource.getDatanodeDetails()),
        containerBalancerTask.getOverUtilizedNodes().stream()
            .map(DatanodeUsageInfo::getDatanodeDetails).collect(Collectors.toSet()));
    assertEquals(Collections.singleton(diskTarget.getDatanodeDetails()),
        containerBalancerTask.getUnderUtilizedNodes().stream()
            .map(DatanodeUsageInfo::getDatanodeDetails).collect(Collectors.toSet()));
    assertEquals(2, containerBalancerTask.getMetrics().getNumDatanodesUnbalanced());
    assertEquals(ContainerBalancerStopReason.CAN_NOT_BALANCE_ANY_MORE.name(), containerBalancerTask.getStopReason());
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  public void testNodeLimitDoesNotRestoreTargetsFromAnotherStorageType(int maxNodes) throws Exception {
    DatanodeUsageInfo ssdSource = storageTypeNode(80, -1);
    DatanodeUsageInfo ssdTarget = storageTypeNode(20, -1);
    DatanodeUsageInfo diskSource = storageTypeNode(-1, 80);
    DatanodeUsageInfo diskTarget = storageTypeNode(-1, 20);
    addStorageTypeContainer(ssdSource, 1000, StorageType.SSD, STORAGE_UNIT);
    addStorageTypeContainer(diskSource, 1001, StorageType.DISK, STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(ssdSource, ssdTarget, diskSource, diskTarget), 0, order);
    balancerConfiguration.setMaxDatanodesPercentageToInvolvePerIteration(maxNodes * 25);
    startBalancer(balancerConfiguration);

    if (maxNodes < 4) {
      assertThat(order).containsExactly(StorageType.SSD);
    } else {
      assertThat(order).containsExactly(StorageType.SSD, StorageType.DISK);
      assertEquals(diskTarget.getDatanodeDetails(),
          containerBalancerTask.getContainerToTargetMap().get(ContainerID.valueOf(1001)));
    }
    assertEquals(ssdTarget.getDatanodeDetails(),
        containerBalancerTask.getContainerToTargetMap().get(ContainerID.valueOf(1000)));
    assertThat(containerBalancerTask.getMetrics().getNumDatanodesInvolvedInLatestIteration())
        .isLessThanOrEqualTo(maxNodes);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testTrafficLimitIsSharedAcrossStorageTypes(boolean limitEntering) throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, 60);
    addStorageTypeContainer(source, 1001, StorageType.DISK, 60);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    if (limitEntering) {
      balancerConfiguration.setMaxSizeEnteringTarget(100);
    } else {
      balancerConfiguration.setMaxSizeLeavingSource(100);
    }
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.SSD);
    assertEquals(60, containerBalancerTask.getSizeScheduledForMoveInLatestIteration());
  }

  @Test
  public void testDatanodeParticipatingInBothRolesIsCountedOnce() throws Exception {
    DatanodeUsageInfo first = storageTypeNode(80, 20);
    DatanodeUsageInfo second = storageTypeNode(20, 80);
    addStorageTypeContainer(first, 1000, StorageType.SSD, STORAGE_UNIT);
    addStorageTypeContainer(second, 1001, StorageType.DISK, STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(first, second), 0, order);
    startBalancer(balancerConfiguration);

    assertThat(order).containsExactly(StorageType.SSD, StorageType.DISK);
    assertEquals(2, containerBalancerTask.getMetrics().getNumDatanodesInvolvedInLatestIteration());
  }

  @Test
  public void testNoMoveWhenDifferentTypesHaveNoSourceTargetPair() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, -1);
    DatanodeUsageInfo target = storageTypeNode(-1, 20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).isEmpty();
    assertThat(containerBalancerTask.getContainerToSourceMap()).isEmpty();
  }

  @Test
  public void testStorageTypesTakeTurnsWithinIteration() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20, 20);
    List<StorageType> types = Arrays.asList(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE);
    for (StorageType type : types) {
      addStorageTypeContainer(source, 1000L + type.ordinal() * 10, type, STORAGE_UNIT);
      addStorageTypeContainer(source, 1001L + type.ordinal() * 10, type, STORAGE_UNIT);
    }
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).containsExactly(StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE,
        StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE);
    assertEquals(2, containerBalancerTask.getMetrics().getNumDatanodesInvolvedInLatestIteration());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUnknownOrConflictingReplicaIsNotUsedForTypedMoves(boolean conflicting) throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20);
    addStorageTypeContainer(source, 1000, conflicting ? StorageType.SSD : null, STORAGE_UNIT);
    if (conflicting) {
      ContainerID id = ContainerID.valueOf(1000);
      ContainerReplica replica = cidToReplicasMap.get(id).iterator().next().toBuilder()
          .setVolumeStorageType(StorageType.DISK).build();
      cidToReplicasMap.put(id, Collections.singleton(replica));
    }
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).isEmpty();
    assertThat(containerBalancerTask.getContainerToSourceMap()).isEmpty();
    assertEquals(0, containerBalancerTask.getMetrics().getNumContainerMovesScheduledInLatestIteration());
  }

  @Test
  public void testLegacyReplicaUsesReportedVolumeType() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    ContainerID id = ContainerID.valueOf(1000);
    cidToReplicasMap.put(id, Collections.singleton(cidToReplicasMap.get(id).iterator().next().toBuilder()
        .setStorageType(null).build()));
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(source, target), 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).containsExactly(StorageType.SSD);
  }

  @Test
  public void testAggregateOnlyReportsKeepLegacyMoveApi() throws Exception {
    for (DatanodeUsageInfo node : nodesInCluster) {
      SCMNodeStat typed = node.getScmNodeStat();
      SCMNodeStat aggregate = new SCMNodeStat();
      aggregate.getCapacity().set(typed.getCapacity().get());
      aggregate.getScmUsed().set(typed.getScmUsed().get());
      aggregate.getRemaining().set(typed.getRemaining().get());
      node.setScmNodeStat(aggregate);
    }
    configureStorageTypeCluster(nodesInCluster, 0, new ArrayList<>());
    cidToReplicasMap.replaceAll((id, replicas) -> replicas.stream().map(replica ->
        replica.toBuilder().setStorageType(null).setVolumeStorageType(null).build()).collect(Collectors.toSet()));
    when(moveManager.move(any(ContainerID.class), any(DatanodeDetails.class), any(DatanodeDetails.class)))
        .thenReturn(CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED));

    startBalancer(balancerConfiguration);

    assertThat(containerBalancerTask.getContainerToSourceMap()).isNotEmpty();
    verify(moveManager, atLeast(1)).move(any(ContainerID.class),
        any(DatanodeDetails.class), any(DatanodeDetails.class));
    verify(moveManager, org.mockito.Mockito.never()).move(any(), any(), any(), any());
    // Usage that is not reported by storage type gives results that are reported under the unknown type.
    List<ContainerBalancerStorageTypeStatusInfoProto> reported =
        containerBalancerTask.getCurrentIterationsStatistic().get(0).getStorageTypeStatusInfo();
    assertEquals(1, reported.size());
    assertThat(reported.get(0).hasStorageType()).isFalse();
    assertEquals(containerBalancerTask.getMetrics().getNumContainerMovesScheduledInLatestIteration(),
        reported.get(0).getContainerMovesScheduled());
    assertEquals(containerBalancerTask.getMetrics().getNumContainerMovesCompletedInLatestIteration(),
        reported.get(0).getContainerMovesCompleted());
  }

  @Test
  public void testIncompleteStorageTypeUsageStopsBeforeSelectingMoves() throws Exception {
    SCMNodeStat legacyStat = new SCMNodeStat();
    legacyStat.getCapacity().set(100 * STORAGE_UNIT);
    legacyStat.getScmUsed().set(50 * STORAGE_UNIT);
    legacyStat.getRemaining().set(50 * STORAGE_UNIT);
    DatanodeUsageInfo legacy = new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(), legacyStat);
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(Arrays.asList(storageTypeNode(80, 80), storageTypeNode(20, 20), legacy), 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).isEmpty();
    assertThat(containerBalancerTask.getStopMessage()).contains("Incomplete storage-type usage reports");
  }

  @Test
  public void testSchedulingUsesAllSourcesRatherThanDisplayLimit() throws Exception {
    List<DatanodeUsageInfo> nodes = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      DatanodeUsageInfo source = storageTypeNode(80);
      nodes.add(source);
      nodes.add(storageTypeNode(20));
      addStorageTypeContainer(source, 1000 + i, StorageType.SSD, STORAGE_UNIT);
    }
    List<StorageType> order = new ArrayList<>();
    configureStorageTypeCluster(nodes, 0, order);
    startBalancer(balancerConfiguration);
    assertThat(order).hasSize(7).containsOnly(StorageType.SSD);
    assertThat(containerBalancerTask.getContainerToSourceMap()).hasSize(7);
  }

  /** A result that arrives while another storage type has its turn is still counted under the type that moved. */
  @ParameterizedTest
  @EnumSource(value = MoveManager.MoveResult.class,
      names = {"COMPLETED", "REPLICATION_NOT_HEALTHY_AFTER_MOVE"})
  public void testMoveResultsKeepScheduledStorageTypeAfterSwitchingTurns(MoveManager.MoveResult lateResult)
      throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    addStorageTypeContainer(source, 1001, StorageType.DISK, STORAGE_UNIT);
    configureStorageTypeCluster(Arrays.asList(source, target), 0, new ArrayList<>());
    CompletableFuture<MoveManager.MoveResult> ssdMove = new CompletableFuture<>();
    doAnswer(invocation -> {
      if (invocation.<StorageType>getArgument(3) == StorageType.SSD) {
        return ssdMove;
      }
      // The SSD move finishes while DISK has its turn.
      ssdMove.complete(lateResult);
      return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    }).when(moveManager).move(any(), any(), any(), any());

    startBalancer(balancerConfiguration);

    boolean completed = lateResult == MoveManager.MoveResult.COMPLETED;
    ContainerBalancerStorageTypeStatusInfoProto ssd = typeStatus(0, StorageType.SSD);
    assertEquals(1, ssd.getContainerMovesScheduled());
    assertEquals(completed ? 1 : 0, ssd.getContainerMovesCompleted());
    assertEquals(completed ? 0 : 1, ssd.getContainerMovesFailed());
    assertEquals(completed ? STORAGE_UNIT : 0, ssd.getDataSizeMoved());
    ContainerBalancerStorageTypeStatusInfoProto disk = typeStatus(0, StorageType.DISK);
    assertEquals(1, disk.getContainerMovesScheduled());
    assertEquals(1, disk.getContainerMovesCompleted());
    assertEquals(0, disk.getContainerMovesFailed());
    ContainerBalancerMetrics overall = containerBalancerTask.getMetrics();
    assertEquals(completed ? 2 : 1, overall.getNumContainerMovesCompletedInLatestIteration());
    assertEquals(completed ? 0 : 1, overall.getNumContainerMovesFailedInLatestIteration());
  }

  /** A move that timed out stays a timeout when it completes after its iteration has ended. */
  @Test
  public void testTimedOutMoveIsNotCountedAgainWhenItCompletesLater() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80);
    DatanodeUsageInfo target = storageTypeNode(20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    balancerConfiguration.setMoveTimeout(300);
    configureStorageTypeCluster(Arrays.asList(source, target), 0, new ArrayList<>());
    CompletableFuture<MoveManager.MoveResult> move = new CompletableFuture<>();
    doAnswer(invocation -> move).when(moveManager).move(any(), any(), any(), any());

    startBalancer(balancerConfiguration);
    move.complete(MoveManager.MoveResult.COMPLETED);

    ContainerBalancerStorageTypeStatusInfoProto ssd = typeStatus(0, StorageType.SSD);
    assertEquals(1, ssd.getContainerMovesScheduled());
    assertEquals(1, ssd.getContainerMovesTimeout());
    assertEquals(0, ssd.getContainerMovesCompleted());
    assertEquals(0, ssd.getDataSizeMoved());
    ContainerBalancerMetrics overall = containerBalancerTask.getMetrics();
    assertEquals(1, overall.getNumContainerMovesTimeoutInLatestIteration());
    assertEquals(0, overall.getNumContainerMovesCompletedInLatestIteration());
    assertEquals(0, overall.getDataSizeMovedInLatestIteration());
    assertEquals(1, overall.getNumContainerMovesTimeout());
    assertEquals(0, overall.getNumContainerMovesCompleted());
  }

  /**
   * A move that is still running when the iteration ends, and is not cancelled, must not change any count when it
   * completes later. The wait for the moves is interrupted here, so that the iteration ends without cancelling them.
   */
  @Test
  public void testMoveCompletingAfterItsIterationEndedIsNotCounted() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80);
    DatanodeUsageInfo target = storageTypeNode(20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    configureStorageTypeCluster(Arrays.asList(source, target), 0, new ArrayList<>());
    CompletableFuture<MoveManager.MoveResult> move = new CompletableFuture<>();
    doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      return move;
    }).when(moveManager).move(any(), any(), any(), any());

    startBalancer(balancerConfiguration);
    assertThat(Thread.interrupted()).isTrue();
    assertFalse(move.isDone());
    move.complete(MoveManager.MoveResult.COMPLETED);

    ContainerBalancerStorageTypeStatusInfoProto ssd = typeStatus(0, StorageType.SSD);
    assertEquals(1, ssd.getContainerMovesScheduled());
    assertEquals(0, ssd.getContainerMovesCompleted());
    assertEquals(0, ssd.getDataSizeMoved());
    ContainerBalancerMetrics overall = containerBalancerTask.getMetrics();
    assertEquals(0, overall.getNumContainerMovesCompletedInLatestIteration());
    assertEquals(0, overall.getDataSizeMovedInLatestIteration());
    assertEquals(0, overall.getNumContainerMovesCompleted());
  }

  /** A move of the first iteration that completes during the second one does not change the second iteration. */
  @Test
  public void testResultOfEarlierIterationDoesNotChangeLaterIteration() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80);
    DatanodeUsageInfo target = storageTypeNode(20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    balancerConfiguration.setMoveTimeout(300);
    balancerConfiguration.setBalancingInterval(0);
    configureStorageTypeCluster(Arrays.asList(source, target), 0, new ArrayList<>());
    balancerConfiguration.setIterations(2);
    CompletableFuture<MoveManager.MoveResult> firstIterationMove = new CompletableFuture<>();
    AtomicInteger moves = new AtomicInteger();
    doAnswer(invocation -> {
      if (moves.incrementAndGet() == 1) {
        return firstIterationMove;
      }
      firstIterationMove.complete(MoveManager.MoveResult.COMPLETED);
      return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    }).when(moveManager).move(any(), any(), any(), any());

    startBalancer(balancerConfiguration);

    assertEquals(2, moves.get());
    ContainerBalancerStorageTypeStatusInfoProto first = typeStatus(0, StorageType.SSD);
    assertEquals(1, first.getContainerMovesTimeout());
    assertEquals(0, first.getContainerMovesCompleted());
    ContainerBalancerStorageTypeStatusInfoProto second = typeStatus(1, StorageType.SSD);
    assertEquals(1, second.getContainerMovesScheduled());
    assertEquals(1, second.getContainerMovesCompleted());
    assertEquals(0, second.getContainerMovesTimeout());
    assertEquals(STORAGE_UNIT, second.getDataSizeMoved());
    ContainerBalancerMetrics overall = containerBalancerTask.getMetrics();
    assertEquals(1, overall.getNumContainerMovesCompleted());
    assertEquals(1, overall.getNumContainerMovesTimeout());
    assertEquals(1, overall.getNumContainerMovesCompletedInLatestIteration());
    assertEquals(STORAGE_UNIT, overall.getDataSizeMovedInLatestIteration());
  }

  /** The counts of the storage types add up to the overall counts, but a datanode is counted once overall. */
  @Test
  public void testStorageTypeCountsAddUpToOverallCounts() throws Exception {
    DatanodeUsageInfo source = storageTypeNode(80, 80, 80);
    DatanodeUsageInfo target = storageTypeNode(20, 20, 20);
    addStorageTypeContainer(source, 1000, StorageType.SSD, STORAGE_UNIT);
    addStorageTypeContainer(source, 1001, StorageType.DISK, STORAGE_UNIT);
    addStorageTypeContainer(source, 1002, StorageType.ARCHIVE, STORAGE_UNIT);
    balancerConfiguration.setMoveTimeout(300);
    configureStorageTypeCluster(Arrays.asList(source, target), 0, new ArrayList<>());
    doAnswer(invocation -> {
      switch (invocation.<StorageType>getArgument(3)) {
      case SSD:
        return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
      case DISK:
        return CompletableFuture.completedFuture(MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE);
      default:
        return new CompletableFuture<MoveManager.MoveResult>();
      }
    }).when(moveManager).move(any(), any(), any(), any());

    startBalancer(balancerConfiguration);

    ContainerBalancerStorageTypeStatusInfoProto ssd = typeStatus(0, StorageType.SSD);
    ContainerBalancerStorageTypeStatusInfoProto disk = typeStatus(0, StorageType.DISK);
    ContainerBalancerStorageTypeStatusInfoProto archive = typeStatus(0, StorageType.ARCHIVE);
    assertEquals(1, ssd.getContainerMovesCompleted());
    assertEquals(1, disk.getContainerMovesFailed());
    assertEquals(1, archive.getContainerMovesTimeout());

    ContainerBalancerMetrics overall = containerBalancerTask.getMetrics();
    List<ContainerBalancerStorageTypeStatusInfoProto> all = Arrays.asList(ssd, disk, archive);
    assertEquals(overall.getNumContainerMovesScheduledInLatestIteration(),
        all.stream().mapToLong(ContainerBalancerStorageTypeStatusInfoProto::getContainerMovesScheduled).sum());
    assertEquals(overall.getNumContainerMovesCompletedInLatestIteration(),
        all.stream().mapToLong(ContainerBalancerStorageTypeStatusInfoProto::getContainerMovesCompleted).sum());
    assertEquals(overall.getNumContainerMovesFailedInLatestIteration(),
        all.stream().mapToLong(ContainerBalancerStorageTypeStatusInfoProto::getContainerMovesFailed).sum());
    assertEquals(overall.getNumContainerMovesTimeoutInLatestIteration(),
        all.stream().mapToLong(ContainerBalancerStorageTypeStatusInfoProto::getContainerMovesTimeout).sum());
    assertEquals(overall.getDataSizeMovedInLatestIteration(),
        all.stream().mapToLong(ContainerBalancerStorageTypeStatusInfoProto::getDataSizeMoved).sum());
    // One source and one target datanode took part. SSD and ARCHIVE moved data between them; the DISK move failed.
    assertEquals(2, overall.getNumDatanodesInvolvedInLatestIteration());
    assertEquals(2, ssd.getNumDatanodesInvolved());
    assertEquals(0, disk.getNumDatanodesInvolved());
    assertEquals(2, archive.getNumDatanodesInvolved());
    assertEquals(2, ssd.getNumDatanodesUnbalanced());

    // The finished iteration is added to the exported totals of each storage type.
    assertEquals(1, exportedMetric(StorageType.SSD, "NumContainerMovesCompleted"));
    assertEquals(STORAGE_UNIT, exportedMetric(StorageType.SSD, "DataSizeMovedBytes"));
    assertEquals(1, exportedMetric(StorageType.DISK, "NumContainerMovesFailed"));
    assertEquals(1, exportedMetric(StorageType.ARCHIVE, "NumContainerMovesTimeout"));
  }

  private long exportedMetric(StorageType type, String name) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    containerBalancerTask.getMetrics().storageTypeMetrics().getMetrics(collector, true);
    for (MetricsRecordImpl record : collector.getRecords()) {
      if (record.tags().stream().anyMatch(tag -> tag.name().equals("StorageType") && tag.value().equals(type.name()))) {
        for (AbstractMetric metric : record.metrics()) {
          if (metric.name().equals(name)) {
            return metric.value().longValue();
          }
        }
        throw new AssertionError("No metric " + name);
      }
    }
    throw new AssertionError("No metrics of " + type);
  }

  private ContainerBalancerStorageTypeStatusInfoProto typeStatus(int iteration, StorageType type) {
    return containerBalancerTask.getCurrentIterationsStatistic().get(iteration).getStorageTypeStatusInfo().stream()
        .filter(status -> status.hasStorageType() && status.getStorageType().name().equals(type.name()))
        .findFirst().orElseThrow(() -> new AssertionError("No status of " + type + " in iteration " + iteration));
  }

  private DatanodeUsageInfo storageTypeNode(long... usedPercentages) {
    StorageType[] types = {StorageType.SSD, StorageType.DISK, StorageType.ARCHIVE};
    Map<StorageType, Long> capacity = new EnumMap<>(StorageType.class);
    Map<StorageType, Long> used = new EnumMap<>(StorageType.class);
    Map<StorageType, Long> remaining = new EnumMap<>(StorageType.class);
    for (int i = 0; i < usedPercentages.length; i++) {
      if (usedPercentages[i] >= 0) {
        capacity.put(types[i], 100 * STORAGE_UNIT);
        used.put(types[i], usedPercentages[i] * STORAGE_UNIT);
        remaining.put(types[i], (100 - usedPercentages[i]) * STORAGE_UNIT);
      }
    }
    return new DatanodeUsageInfo(MockDatanodeDetails.randomDatanodeDetails(), new SCMNodeStat(capacity, used, remaining,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()));
  }

  /** Adds one container with a replica on each given node; each replica records and reports the given storage type. */
  private void addMixedStorageTypeContainer(long id, long size, Map<DatanodeUsageInfo, StorageType> replicaTypes) {
    ContainerInfo container = createContainer(id, 1);
    container.setUsedBytes(size);
    cidToInfoMap.put(container.containerID(), container);
    Set<ContainerReplica> replicas = new HashSet<>();
    replicaTypes.forEach((node, type) -> replicas.add(storageTypeReplica(container.containerID(), node, type, type,
        size)));
    cidToReplicasMap.put(container.containerID(), replicas);
  }

  private ContainerReplica storageTypeReplica(ContainerID id, DatanodeUsageInfo node, StorageType recordedType,
      StorageType volumeType, long size) {
    return ContainerReplica.newBuilder().setContainerID(id).setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(node.getDatanodeDetails()).setOriginNodeId(node.getDatanodeDetails().getID())
        .setStorageType(recordedType).setVolumeStorageType(volumeType).setBytesUsed(size).setSequenceId(1000L).build();
  }

  private void addStorageTypeContainer(DatanodeUsageInfo source, long id, StorageType type, long size) {
    ContainerInfo container = createContainer(id, 1);
    container.setUsedBytes(size);
    cidToInfoMap.put(container.containerID(), container);
    ContainerReplica replica = ContainerReplica.newBuilder().setContainerID(container.containerID())
        .setContainerState(ContainerReplicaProto.State.CLOSED).setDatanodeDetails(source.getDatanodeDetails())
        .setStorageType(type).setVolumeStorageType(type).setBytesUsed(size).setSequenceId(1000L).build();
    cidToReplicasMap.put(container.containerID(), Collections.singleton(replica));
  }

  private void configureStorageTypeCluster(List<DatanodeUsageInfo> nodes, int iteration, List<StorageType> order)
      throws Exception {
    NodeManager nodeManager = mock(NodeManager.class);
    when(nodeManager.getMostOrLeastUsedDatanodes(true)).thenAnswer(invocation -> new ArrayList<>(nodes));
    when(nodeManager.getUsageInfo(any(DatanodeDetails.class))).thenAnswer(invocation -> nodes.stream()
        .filter(node -> node.getDatanodeDetails().equals(invocation.getArgument(0))).findFirst().get());
    when(nodeManager.isNodeRegistered(any(DatanodeDetails.class))).thenReturn(true);
    when(nodeManager.getContainers(any(DatanodeDetails.class))).thenAnswer(invocation -> {
      Set<ContainerID> containers = new HashSet<>();
      cidToReplicasMap.forEach((id, replicas) -> {
        if (replicas.stream().anyMatch(replica -> replica.getDatanodeDetails().equals(invocation.getArgument(0)))) {
          containers.add(id);
        }
      });
      return containers;
    });
    when(scm.getScmNodeManager()).thenReturn(nodeManager);
    PlacementPolicyValidateProxy placement = mock(PlacementPolicyValidateProxy.class);
    ContainerPlacementStatus status = mock(ContainerPlacementStatus.class);
    when(status.isPolicySatisfied()).thenReturn(true);
    when(placement.validateContainerPlacement(any(), any())).thenReturn(status);
    when(scm.getPlacementPolicyValidateProxy()).thenReturn(placement);
    when(moveManager.move(any(), any(), any(), any())).thenAnswer(invocation -> {
      ContainerID id = invocation.getArgument(0);
      StorageType type = invocation.getArgument(3);
      assertEquals(cidToReplicasMap.get(id).iterator().next().getTargetStorageTypeForCopy(), type);
      order.add(type);
      return CompletableFuture.completedFuture(MoveManager.MoveResult.COMPLETED);
    });
    balancerConfiguration.setIterations(iteration + 1);
    balancerConfiguration.setMaxSizeToMovePerIteration(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeEnteringTarget(100 * STORAGE_UNIT);
    balancerConfiguration.setMaxSizeLeavingSource(100 * STORAGE_UNIT);
    ContainerBalancer balancer = new ContainerBalancer(scm);
    containerBalancerTask = new ContainerBalancerTask(scm, iteration, balancer, balancer.getMetrics(),
        balancerConfiguration, false);
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
