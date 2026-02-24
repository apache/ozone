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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.CapacityPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to validate the WritableECContainerProvider works correctly.
 */
public class TestWritableECContainerProvider {

  private static final String OWNER = "SCM";
  private PipelineManager pipelineManager;
  private final ContainerManager containerManager
      = mock(ContainerManager.class);

  private OzoneConfiguration conf;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;
  private static MockNodeManager nodeManager;
  private WritableContainerProvider<ECReplicationConfig> provider;
  private ECReplicationConfig repConfig;

  private Map<ContainerID, ContainerInfo> containers;
  private WritableECContainerProviderConfig providerConf;

  public static Collection<PipelineChoosePolicy> policies() {
    Collection<PipelineChoosePolicy> policies = new ArrayList<>();
    // init nodeManager
    NodeSchemaManager.getInstance().init(new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA}, true);
    NetworkTopologyImpl cluster =
        new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    int count = 10;
    List<DatanodeDetails> datanodes = IntStream.range(0, count)
        .mapToObj(i -> MockDatanodeDetails.randomDatanodeDetails())
        .collect(Collectors.toList());
    nodeManager = new MockNodeManager(cluster, datanodes, false, count);

    policies.add(new RandomPipelineChoosePolicy());
    policies.add(new HealthyPipelineChoosePolicy());
    policies.add(new CapacityPipelineChoosePolicy().init(nodeManager));
    return policies;
  }

  @BeforeEach
  void setup(@TempDir File testDir) throws IOException {
    repConfig = new ECReplicationConfig(3, 2);
    conf = new OzoneConfiguration();

    providerConf = conf.getObject(WritableECContainerProviderConfig.class);

    containers = new HashMap<>();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);

    doAnswer(call -> {
      Pipeline pipeline = (Pipeline)call.getArguments()[2];
      ContainerInfo container = createContainer(pipeline,
          repConfig, System.nanoTime());
      pipelineManager.addContainerToPipeline(
          pipeline.getId(), container.containerID());
      containers.put(container.containerID(), container);
      return container;
    }).when(containerManager).getMatchingContainer(anyLong(),
        anyString(), any(Pipeline.class));

    doAnswer(call ->
        containers.get((ContainerID)call.getArguments()[0]))
        .when(containerManager).getContainer(any(ContainerID.class));

  }

  private WritableContainerProvider<ECReplicationConfig> createSubject(
      PipelineChoosePolicy policy) {
    return createSubject(pipelineManager, policy);
  }

  private WritableContainerProvider<ECReplicationConfig> createSubject(
      PipelineManager customPipelineManager, PipelineChoosePolicy policy) {
    return new WritableECContainerProvider(providerConf, getMaxContainerSize(),
        nodeManager, customPipelineManager, containerManager,
        policy);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void testPipelinesCreatedBasedOnTotalDiskCount(PipelineChoosePolicy policy)
      throws IOException {
    provider = createSubject(policy);
    providerConf.setMinimumPipelines(1);
    nodeManager.setNumHealthyVolumes(20);

    int volumeCount = nodeManager.totalHealthyVolumeCount();
    int pipelineLimit = volumeCount / repConfig.getRequiredNodes();
    Set<ContainerInfo> allocated = assertDistinctContainers(pipelineLimit);
    assertReusesExisting(allocated, pipelineLimit);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void testPipelinesCreatedBasedOnTotalDiskCountWithFactor(
      PipelineChoosePolicy policy) throws IOException {
    provider = createSubject(policy);
    int factor = 10;
    providerConf.setMinimumPipelines(1);
    providerConf.setPipelinePerVolumeFactor(factor);
    nodeManager.setNumHealthyVolumes(5);

    int volumeCount = nodeManager.totalHealthyVolumeCount();
    int pipelineLimit = factor * volumeCount / repConfig.getRequiredNodes();
    Set<ContainerInfo> allocated = assertDistinctContainers(pipelineLimit);
    assertReusesExisting(allocated, pipelineLimit);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void testPipelinesCreatedUpToMinLimitAndRandomPipelineReturned(
      PipelineChoosePolicy policy) throws IOException {
    provider = createSubject(policy);
    int minimumPipelines = providerConf.getMinimumPipelines();
    Set<ContainerInfo> allocated = assertDistinctContainers(minimumPipelines);
    assertReusesExisting(allocated, minimumPipelines);
  }

  private Set<ContainerInfo> assertDistinctContainers(int n)
      throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < n; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertThat(allocatedContainers)
          .withFailMessage("Provided existing container for request " + i)
          .doesNotContain(container);
      allocatedContainers.add(container);
    }
    return allocatedContainers;
  }

  private void assertReusesExisting(Set<ContainerInfo> existing, int n)
      throws IOException {
    for (int i = 0; i < 3 * n; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertThat(existing)
          .withFailMessage("Provided new container for request " + i)
          .contains(container);
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testPiplineLimitIgnoresExcludedPipelines(
      PipelineChoosePolicy policy) throws IOException {
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < providerConf.getMinimumPipelines(); i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude one. It should use
    // one of the existing rather than creating a new one, as the limit is
    // checked against all pipelines, not just the filtered list
    ExcludeList exclude = new ExcludeList();
    PipelineID excludedID = allocatedContainers
        .stream().findFirst().get().getPipelineID();
    exclude.addPipeline(excludedID);

    ContainerInfo c = provider.getContainer(1, repConfig, OWNER, exclude);
    assertNotEquals(excludedID, c.getPipelineID());
    assertThat(allocatedContainers).contains(c);
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewPipelineNotCreatedIfAllPipelinesExcluded(
      PipelineChoosePolicy policy) throws IOException {
    final int nodeCount = nodeManager.getNodeCount(null, null);
    providerConf.setMinimumPipelines(nodeCount);
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < providerConf.getMinimumPipelines(); i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude them all
    ExcludeList exclude = new ExcludeList();
    for (ContainerInfo c : allocatedContainers) {
      exclude.addPipeline(c.getPipelineID());
    }
    assertThrows(IOException.class, () -> provider.getContainer(
        1, repConfig, OWNER, exclude));
  }

  @ParameterizedTest
  @MethodSource("policies")
  void newPipelineCreatedIfSoftLimitReached(PipelineChoosePolicy policy)
      throws IOException {

    providerConf.setMinimumPipelines(1);
    provider = createSubject(policy);
    ContainerInfo container = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList());

    ExcludeList exclude = new ExcludeList();
    exclude.addPipeline(container.getPipelineID());
    exclude.addDatanode(
        pipelineManager.getPipeline(container.getPipelineID()).getFirstNode());

    ContainerInfo newContainer = provider.getContainer(
        1, repConfig, OWNER, exclude);
    assertNotSame(container, newContainer);
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewPipelineNotCreatedIfAllContainersExcluded(
      PipelineChoosePolicy policy) throws IOException {
    final int nodeCount = nodeManager.getNodeCount(null, null);
    providerConf.setMinimumPipelines(nodeCount);
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < providerConf.getMinimumPipelines(); i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude all the associated
    // containers.
    ExcludeList exclude = new ExcludeList();
    for (ContainerInfo c : allocatedContainers) {
      exclude.addConatinerId(c.containerID());
    }
    assertThrows(IOException.class, () -> provider.getContainer(
        1, repConfig, OWNER, exclude));
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testUnableToCreateAnyPipelinesThrowsException(
      PipelineChoosePolicy policy) throws IOException {
    pipelineManager = new MockPipelineManager(
        dbStore, scmhaManager, nodeManager) {
      @Override
      public Pipeline createPipeline(ReplicationConfig repConf,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes) throws IOException {
        throw new IOException("Cannot create pipelines");
      }
    };
    provider = createSubject(policy);

    IOException ioException = assertThrows(IOException.class,
        () -> provider.getContainer(1, repConfig, OWNER, new ExcludeList()));
    assertThat(ioException.getMessage())
        .contains("Cannot create pipelines");
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testExistingPipelineReturnedWhenNewCannotBeCreated(
      PipelineChoosePolicy policy) throws IOException {
    pipelineManager = new MockPipelineManager(
        dbStore, scmhaManager, nodeManager) {

      private boolean throwError = false;

      @Override
      public Pipeline createPipeline(ReplicationConfig repConf,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes)
          throws IOException {
        if (throwError) {
          throw new RocksDatabaseException("Cannot create pipelines");
        }
        throwError = true;
        return super.createPipeline(repConfig);
      }
    };
    provider = createSubject(policy);

    IOException ioException = assertThrows(IOException.class,
        () -> provider.getContainer(1, repConfig, OWNER, new ExcludeList()));
    assertThat(ioException.getMessage())
        .contains("Cannot create pipelines");

    for (int i = 0; i < 5; i++) {
      ioException = assertThrows(IOException.class,
          () -> provider.getContainer(1, repConfig, OWNER, new ExcludeList()));
      assertThat(ioException.getMessage())
          .contains("Cannot create pipelines");
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewContainerAllocatedAndPipelinesClosedIfNoSpaceInExisting(
      PipelineChoosePolicy policy) throws IOException {
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers =
        assertDistinctContainers(providerConf.getMinimumPipelines());
    // Update all the containers to make them nearly full, but with enough space
    // for an EC block to be striped across them.
    for (ContainerInfo c : allocatedContainers) {
      c.setUsedBytes(getMaxContainerSize() - 90 * 1024 * 1024);
    }

    // Get a new container of size 50 and ensure it is one of the original set.
    // We ask for a space of 50 MB, and will actually need 50 MB space.
    ContainerInfo newContainer =
        provider.getContainer(50 * 1024 * 1024, repConfig, OWNER,
            new ExcludeList());
    assertNotNull(newContainer);
    assertThat(allocatedContainers).contains(newContainer);
    // Now get a new container where there is not enough space in the existing
    // and ensure a new container gets created.
    newContainer = provider.getContainer(
        128 * 1024 * 1024, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertThat(allocatedContainers).doesNotContain(newContainer);
    // The original pipelines should all be closed, triggered by the lack of
    // space.
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testPipelineNotFoundWhenAttemptingToUseExisting(
      PipelineChoosePolicy policy) throws IOException {
    // Ensure PM throws PNF exception when we ask for the containers in the
    // pipeline
    pipelineManager = new MockPipelineManager(
        dbStore, scmhaManager, nodeManager) {

      @Override
      public NavigableSet<ContainerID> getContainersInPipeline(PipelineID pipelineID) throws PipelineNotFoundException {
        throw new PipelineNotFoundException("Simulated exception");
      }
    };
    provider = createSubject(policy);

    Set<ContainerInfo> allocatedContainers =
        assertDistinctContainers(providerConf.getMinimumPipelines());

    // Now attempt to get a container - any attempt to use an existing with
    // throw PNF and then we must allocate a new one
    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertThat(allocatedContainers).doesNotContain(newContainer);
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testContainerNotFoundWhenAttemptingToUseExisting(
      PipelineChoosePolicy policy) throws IOException {
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers =
        assertDistinctContainers(providerConf.getMinimumPipelines());

    // Ensure ContainerManager always throws when a container is requested so
    // existing pipelines cannot be used
    doAnswer(call -> {
      throw ContainerNotFoundException.newInstanceForTesting();
    }).when(containerManager).getContainer(any(ContainerID.class));

    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertThat(allocatedContainers).doesNotContain(newContainer);

    // Ensure all the existing pipelines are closed
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testPipelineOpenButContainerRemovedFromIt(
      PipelineChoosePolicy policy) throws IOException {
    // This can happen if the container close process is triggered from the DN.
    // When tha happens, CM will change the container state to CLOSING and
    // remove it from the container list in pipeline Manager.
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < providerConf.getMinimumPipelines(); i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      assertThat(allocatedContainers).doesNotContain(container);
      allocatedContainers.add(container);
      // Remove the container from the pipeline to simulate closing it
      pipelineManager.removeContainerFromPipeline(
          container.getPipelineID(), container.containerID());
    }
    ContainerInfo newContainer = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList());
    assertThat(allocatedContainers).doesNotContain(newContainer);
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  /**
   * Suppose there's a closed container but its pipeline is still open. This
   * pipeline is also present in the excludeList. Such a pipeline should not
   * be included in the count of open pipelines and should be closed.
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-9142">...</a>
   */
  @ParameterizedTest
  @MethodSource("policies")
  public void testExcludedOpenPipelineWithClosedContainerIsClosed(
      PipelineChoosePolicy policy) throws IOException {
    int nodeCount = nodeManager.getNodeCount(
        org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceHealthy());
    providerConf.setMinimumPipelines(nodeCount);
    provider = createSubject(policy);
    Set<ContainerInfo> allocated = assertDistinctContainers(nodeCount);
    assertEquals(nodeCount, allocated.size());

    ExcludeList excludeList = new ExcludeList();
    // close all of these containers
    for (ContainerInfo container : allocated) {
      // Remove the container from the pipeline to simulate closing the
      // container
      pipelineManager.removeContainerFromPipeline(
          container.getPipelineID(), container.containerID());
      excludeList.addPipeline(container.getPipelineID());
    }

    // expecting a new container to be created
    ContainerInfo containerInfo = provider.getContainer(1, repConfig, OWNER,
        excludeList);
    assertThat(allocated).doesNotContain(containerInfo);
    for (ContainerInfo c : allocated) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testExcludedNodesPassedToCreatePipelineIfProvided(
      PipelineChoosePolicy policy) throws IOException {
    PipelineManager pipelineManagerSpy = spy(pipelineManager);
    provider = createSubject(pipelineManagerSpy, policy);
    ExcludeList excludeList = new ExcludeList();

    // EmptyList should be passed if there are no nodes excluded.
    ContainerInfo container = provider.getContainer(
        1, repConfig, OWNER, excludeList);
    assertNotNull(container);

    verify(pipelineManagerSpy).createPipeline(repConfig,
        Collections.emptyList(), Collections.emptyList());

    // If nodes are excluded then the excluded nodes should be passed through to
    // the create pipeline call.
    excludeList.addDatanode(MockDatanodeDetails.randomDatanodeDetails());
    List<DatanodeDetails> excludedNodes =
        new ArrayList<>(excludeList.getDatanodes());

    container = provider.getContainer(
        1, repConfig, OWNER, excludeList);
    assertNotNull(container);
    verify(pipelineManagerSpy).createPipeline(repConfig, excludedNodes,
        Collections.emptyList());
  }

  private ContainerInfo createContainer(Pipeline pipeline,
      ReplicationConfig repConf, long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setOwner(OWNER)
        .setReplicationConfig(repConf)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setNumberOfKeys(0)
        .setUsedBytes(0)
        .setSequenceId(0)
        .setDeleteTransactionId(0)
        .build();
  }

  private long getMaxContainerSize() {
    return (long)conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, BYTES);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void testNullStorageTypeSkipsFilter(PipelineChoosePolicy policy)
      throws IOException {
    provider = createSubject(policy);
    // Null storageType should behave identically to the 4-param method —
    // both succeed without error
    ContainerInfo container4 = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList());
    assertNotNull(container4);

    ContainerInfo container5 = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), null);
    assertNotNull(container5);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void testStorageTypeDiskMatchesDefaultNodes(PipelineChoosePolicy policy)
      throws IOException {
    provider = createSubject(policy);
    // MockNodeManager reports DISK by default (null StorageTypeProto
    // defaults to DISK), so filtering for DISK should succeed
    ContainerInfo container = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), StorageType.DISK);
    assertNotNull(container);
  }

  /**
   * Tests that StorageType filtering actually rejects pipelines whose nodes
   * don't have the requested type. Uses fully mocked components to control
   * exactly which storage types each node reports, and prevents new pipeline
   * creation so the test can't pass via the fallback allocation path.
   */
  @Test
  void testStorageTypeFilterRejectsNonMatchingPipelines()
      throws IOException {
    // Create 5 EC nodes (3+2)
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      nodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < nodes.size(); i++) {
      replicaIndexes.put(nodes.get(i), i + 1);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setNodes(nodes)
        .setReplicaIndexes(replicaIndexes)
        .build();

    ContainerInfo containerInfo = createContainer(pipeline, repConfig, 1L);

    // Mock PipelineManager: one open pipeline, createPipeline always fails
    PipelineManager pm = mock(PipelineManager.class);
    when(pm.getPipelineCount(repConfig, Pipeline.PipelineState.OPEN))
        .thenReturn(100); // at max, skip initial allocateContainer
    when(pm.getPipelines(repConfig, Pipeline.PipelineState.OPEN))
        .thenReturn(new ArrayList<>(Collections.singletonList(pipeline)));
    when(pm.getContainersInPipeline(pipeline.getId()))
        .thenReturn(new java.util.TreeSet<>(
            Collections.singleton(containerInfo.containerID())));

    // Mock ContainerManager
    ContainerManager cm = mock(ContainerManager.class);
    when(cm.getContainer(containerInfo.containerID()))
        .thenReturn(containerInfo);
    when(cm.getMatchingContainer(anyLong(), anyString(), any(Pipeline.class)))
        .thenReturn(containerInfo);

    // Mock NodeManager: all nodes report only DISK
    NodeManager nm = mock(NodeManager.class);
    when(nm.getNodes(NodeStatus.inServiceHealthy())).thenReturn(nodes);
    when(nm.getNodeCount(NodeStatus.inServiceHealthy())).thenReturn(
        nodes.size());
    for (DatanodeDetails dn : nodes) {
      DatanodeInfo info = mock(DatanodeInfo.class);
      when(info.getStorageReports()).thenReturn(
          Collections.singletonList(createDiskStorageReport()));
      when(nm.getDatanodeInfo(dn)).thenReturn(info);
    }

    WritableECContainerProvider ecProvider = new WritableECContainerProvider(
        providerConf, getMaxContainerSize(), nm, pm, cm,
        new RandomPipelineChoosePolicy());

    // null storageType: filter is no-op, should return the container
    ContainerInfo result = ecProvider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), null);
    assertNotNull(result);
    assertEquals(containerInfo.containerID(), result.containerID());

    // DISK storageType: all nodes have DISK, pipeline should pass filter
    result = ecProvider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), StorageType.DISK);
    assertNotNull(result);
    assertEquals(containerInfo.containerID(), result.containerID());

    // SSD storageType: no nodes have SSD, filter removes the pipeline.
    // Since openPipelineCount >= maximumPipelines and nodeCount <=
    // maximumPipelines, the fallback allocateContainer is also blocked.
    assertThrows(IOException.class,
        () -> ecProvider.getContainer(
            1, repConfig, OWNER, new ExcludeList(), StorageType.SSD));
  }

  /**
   * Tests that filtering works correctly with a mix of SSD and DISK
   * pipelines — the SSD pipeline is returned when SSD is requested, and
   * the DISK pipeline is returned when DISK is requested.
   */
  @Test
  void testStorageTypeFilterSelectsCorrectPipeline()
      throws IOException {
    // Create two sets of nodes: SSD nodes and DISK nodes
    List<DatanodeDetails> ssdNodes = new ArrayList<>();
    List<DatanodeDetails> diskNodes = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ssdNodes.add(MockDatanodeDetails.randomDatanodeDetails());
      diskNodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }

    Map<DatanodeDetails, Integer> ssdIndexes = new HashMap<>();
    Map<DatanodeDetails, Integer> diskIndexes = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      ssdIndexes.put(ssdNodes.get(i), i + 1);
      diskIndexes.put(diskNodes.get(i), i + 1);
    }

    Pipeline ssdPipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setNodes(ssdNodes)
        .setReplicaIndexes(ssdIndexes)
        .build();

    Pipeline diskPipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setNodes(diskNodes)
        .setReplicaIndexes(diskIndexes)
        .build();

    ContainerInfo ssdContainer = createContainer(ssdPipeline, repConfig, 1L);
    ContainerInfo diskContainer = createContainer(diskPipeline, repConfig, 2L);

    // Mock PipelineManager with both pipelines
    PipelineManager pm = mock(PipelineManager.class);
    when(pm.getPipelineCount(repConfig, Pipeline.PipelineState.OPEN))
        .thenReturn(100);
    when(pm.getPipelines(repConfig, Pipeline.PipelineState.OPEN))
        .thenReturn(new ArrayList<>(
            java.util.Arrays.asList(ssdPipeline, diskPipeline)));
    when(pm.getContainersInPipeline(ssdPipeline.getId()))
        .thenReturn(new java.util.TreeSet<>(
            Collections.singleton(ssdContainer.containerID())));
    when(pm.getContainersInPipeline(diskPipeline.getId()))
        .thenReturn(new java.util.TreeSet<>(
            Collections.singleton(diskContainer.containerID())));

    // Mock ContainerManager
    ContainerManager cm = mock(ContainerManager.class);
    when(cm.getContainer(ssdContainer.containerID()))
        .thenReturn(ssdContainer);
    when(cm.getContainer(diskContainer.containerID()))
        .thenReturn(diskContainer);

    // Mock NodeManager
    List<DatanodeDetails> allNodes = new ArrayList<>();
    allNodes.addAll(ssdNodes);
    allNodes.addAll(diskNodes);
    NodeManager nm = mock(NodeManager.class);
    when(nm.getNodes(NodeStatus.inServiceHealthy())).thenReturn(allNodes);
    when(nm.getNodeCount(NodeStatus.inServiceHealthy()))
        .thenReturn(allNodes.size());
    for (DatanodeDetails dn : ssdNodes) {
      DatanodeInfo info = mock(DatanodeInfo.class);
      when(info.getStorageReports()).thenReturn(
          Collections.singletonList(createSsdStorageReport()));
      when(nm.getDatanodeInfo(dn)).thenReturn(info);
    }
    for (DatanodeDetails dn : diskNodes) {
      DatanodeInfo info = mock(DatanodeInfo.class);
      when(info.getStorageReports()).thenReturn(
          Collections.singletonList(createDiskStorageReport()));
      when(nm.getDatanodeInfo(dn)).thenReturn(info);
    }

    WritableECContainerProvider ecProvider = new WritableECContainerProvider(
        providerConf, getMaxContainerSize(), nm, pm, cm,
        new RandomPipelineChoosePolicy());

    // Request SSD: should get the SSD pipeline's container
    ContainerInfo result = ecProvider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), StorageType.SSD);
    assertEquals(ssdPipeline.getId(), result.getPipelineID());

    // Request DISK: should get the DISK pipeline's container
    result = ecProvider.getContainer(
        1, repConfig, OWNER, new ExcludeList(), StorageType.DISK);
    assertEquals(diskPipeline.getId(), result.getPipelineID());
  }

  private static StorageReportProto createDiskStorageReport() {
    return StorageReportProto.newBuilder()
        .setStorageUuid("uuid-" + java.util.UUID.randomUUID())
        .setStorageLocation("/data")
        .setCapacity(100L * 1024 * 1024 * 1024)
        .setScmUsed(10L * 1024 * 1024 * 1024)
        .setRemaining(90L * 1024 * 1024 * 1024)
        .setStorageType(StorageTypeProto.DISK)
        .build();
  }

  private static StorageReportProto createSsdStorageReport() {
    return StorageReportProto.newBuilder()
        .setStorageUuid("uuid-" + java.util.UUID.randomUUID())
        .setStorageLocation("/ssd")
        .setCapacity(100L * 1024 * 1024 * 1024)
        .setScmUsed(10L * 1024 * 1024 * 1024)
        .setRemaining(90L * 1024 * 1024 * 1024)
        .setStorageType(StorageTypeProto.SSD)
        .build();
  }

}
