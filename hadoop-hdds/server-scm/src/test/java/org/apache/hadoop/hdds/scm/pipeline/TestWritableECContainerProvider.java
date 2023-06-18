/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

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
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

/**
 * Tests to validate the WritableECContainerProvider works correctly.
 */
public class TestWritableECContainerProvider {

  private static final String OWNER = "SCM";
  private PipelineManager pipelineManager;
  private final ContainerManager containerManager
      = Mockito.mock(ContainerManager.class);

  private OzoneConfiguration conf;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;
  private MockNodeManager nodeManager;
  private WritableContainerProvider<ECReplicationConfig> provider;
  private ECReplicationConfig repConfig;

  private Map<ContainerID, ContainerInfo> containers;
  private WritableECContainerProviderConfig providerConf;

  @Parameterized.Parameters
  public static Collection<PipelineChoosePolicy> policies() {
    Collection<PipelineChoosePolicy> policies = new ArrayList<>();
    policies.add(new RandomPipelineChoosePolicy());
    policies.add(new HealthyPipelineChoosePolicy());
    return policies;
  }

  @BeforeEach
  public void setup() throws IOException {
    repConfig = new ECReplicationConfig(3, 2);
    conf = new OzoneConfiguration();

    providerConf = conf.getObject(WritableECContainerProviderConfig.class);

    containers = new HashMap<>();
    File testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    nodeManager = new MockNodeManager(true, 10);
    pipelineManager =
        new MockPipelineManager(dbStore, scmhaManager, nodeManager);

    Mockito.doAnswer(call -> {
      Pipeline pipeline = (Pipeline)call.getArguments()[2];
      ContainerInfo container = createContainer(pipeline,
          repConfig, System.nanoTime());
      pipelineManager.addContainerToPipeline(
          pipeline.getId(), container.containerID());
      containers.put(container.containerID(), container);
      return container;
    }).when(containerManager).getMatchingContainer(Mockito.anyLong(),
        Mockito.anyString(), Mockito.any(Pipeline.class));

    Mockito.doAnswer(call ->
        containers.get((ContainerID)call.getArguments()[0]))
        .when(containerManager).getContainer(Mockito.any(ContainerID.class));

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
      throws IOException, TimeoutException {
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
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
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
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    provider = createSubject(policy);
    int minimumPipelines = providerConf.getMinimumPipelines();
    Set<ContainerInfo> allocated = assertDistinctContainers(minimumPipelines);
    assertReusesExisting(allocated, minimumPipelines);
  }

  private Set<ContainerInfo> assertDistinctContainers(int n)
      throws IOException, TimeoutException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < n; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container),
          "Provided existing container for request " + i);
      allocatedContainers.add(container);
    }
    return allocatedContainers;
  }

  private void assertReusesExisting(Set<ContainerInfo> existing, int n)
      throws IOException, TimeoutException {
    for (int i = 0; i < 3 * n; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertTrue(existing.contains(container),
          "Provided new container for request " + i);
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testPiplineLimitIgnoresExcludedPipelines(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
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
    assertTrue(allocatedContainers.contains(c));
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewPipelineNotCreatedIfAllPipelinesExcluded(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
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
      exclude.addPipeline(c.getPipelineID());
    }
    assertThrows(IOException.class, () -> provider.getContainer(
        1, repConfig, OWNER, exclude));
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewPipelineNotCreatedIfAllContainersExcluded(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
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

    try {
      provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      Assert.fail();
    } catch (IOException | TimeoutException ex) {
      GenericTestUtils.assertExceptionContains("Cannot create pipelines", ex);
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testExistingPipelineReturnedWhenNewCannotBeCreated(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    pipelineManager = new MockPipelineManager(
        dbStore, scmhaManager, nodeManager) {

      private boolean throwError = false;

      @Override
      public Pipeline createPipeline(ReplicationConfig repConf,
          List<DatanodeDetails> excludedNodes,
          List<DatanodeDetails> favoredNodes)
          throws IOException, TimeoutException {
        if (throwError) {
          throw new IOException("Cannot create pipelines");
        }
        throwError = true;
        return super.createPipeline(repConfig);
      }
    };
    provider = createSubject(policy);

    try {
      provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      Assert.fail();
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("Cannot create pipelines", ex);
    }

    for (int i = 0; i < 5; i++) {
      try {
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
        Assert.fail();
      } catch (IOException ex) {
        GenericTestUtils.assertExceptionContains("Cannot create pipelines", ex);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testNewContainerAllocatedAndPipelinesClosedIfNoSpaceInExisting(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
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
    assertTrue(allocatedContainers.contains(newContainer));
    // Now get a new container where there is not enough space in the existing
    // and ensure a new container gets created.
    newContainer = provider.getContainer(
        128 * 1024 * 1024, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertFalse(allocatedContainers.contains(newContainer));
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
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    // Ensure PM throws PNF exception when we ask for the containers in the
    // pipeline
    pipelineManager = new MockPipelineManager(
        dbStore, scmhaManager, nodeManager) {

      @Override
      public NavigableSet<ContainerID> getContainersInPipeline(
          PipelineID pipelineID) throws IOException {
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
    assertFalse(allocatedContainers.contains(newContainer));
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testContainerNotFoundWhenAttemptingToUseExisting(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers =
        assertDistinctContainers(providerConf.getMinimumPipelines());

    // Ensure ContainerManager always throws when a container is requested so
    // existing pipelines cannot be used
    Mockito.doAnswer(call -> {
      throw new ContainerNotFoundException();
    }).when(containerManager).getContainer(Mockito.any(ContainerID.class));

    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertFalse(allocatedContainers.contains(newContainer));

    // Ensure all the existing pipelines are closed
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testPipelineOpenButContainerRemovedFromIt(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    // This can happen if the container close process is triggered from the DN.
    // When tha happens, CM will change the container state to CLOSING and
    // remove it from the container list in pipeline Manager.
    provider = createSubject(policy);
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i = 0; i < providerConf.getMinimumPipelines(); i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
      // Remove the container from the pipeline to simulate closing it
      pipelineManager.removeContainerFromPipeline(
          container.getPipelineID(), container.containerID());
    }
    ContainerInfo newContainer = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList());
    assertFalse(allocatedContainers.contains(newContainer));
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testExcludedNodesPassedToCreatePipelineIfProvided(
      PipelineChoosePolicy policy) throws IOException, TimeoutException {
    PipelineManager pipelineManagerSpy = Mockito.spy(pipelineManager);
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

}
