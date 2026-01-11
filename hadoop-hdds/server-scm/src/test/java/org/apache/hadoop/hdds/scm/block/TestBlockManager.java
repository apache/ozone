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

package org.apache.hadoop.hdds.scm.block;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.lease.LeaseManager;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager {
  private StorageContainerManager scm;
  private MockNodeManager nodeManager;
  private PipelineManagerImpl pipelineManager;
  private BlockManagerImpl blockManager;
  private static final long DEFAULT_BLOCK_SIZE = 128 * MB;
  private EventQueue eventQueue;
  private int numContainerPerOwnerInPipeline;
  private SCMMetadataStore scmMetadataStore;
  private ReplicationConfig replicationConfig;

  @BeforeEach
  void setUp(@TempDir File tempDir) throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf(tempDir);
    numContainerPerOwnerInPipeline = conf.getInt(
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);

    conf.setBoolean(HDDS_SCM_SAFEMODE_ENABLED, false);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 5,
        TimeUnit.SECONDS);

    // Override the default Node Manager and SCMHAManager
    // in SCM with the Mock one.
    nodeManager = new MockNodeManager(true, 10);
    SCMHAManager scmHAManager = SCMHAManagerStub.getInstance(true);

    eventQueue = new EventQueue();
    SCMContext scmContext = SCMContext.emptyContext();
    SCMServiceManager serviceManager = new SCMServiceManager();

    scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmHAManager, scmMetadataStore.getSequenceIdTable());

    pipelineManager =
        PipelineManagerImpl.newPipelineManager(
            conf,
            scmHAManager,
            nodeManager,
            scmMetadataStore.getPipelineTable(),
            eventQueue,
            scmContext,
            serviceManager,
            Clock.system(ZoneOffset.UTC));

    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf, eventQueue);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    ContainerManager containerManager =
        new ContainerManagerImpl(conf,
            scmHAManager,
            sequenceIdGen,
            pipelineManager,
            scmMetadataStore.getContainerTable(),
            new ContainerReplicaPendingOps(
                Clock.system(ZoneId.systemDefault()), null));
    SCMSafeModeManager safeModeManager = new SCMSafeModeManager(conf,
        nodeManager, pipelineManager, containerManager, serviceManager, eventQueue, scmContext);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    configurator.setPipelineManager(pipelineManager);
    configurator.setContainerManager(containerManager);
    configurator.setScmSafeModeManager(safeModeManager);
    configurator.setMetadataStore(scmMetadataStore);
    configurator.setSCMHAManager(scmHAManager);
    configurator.setScmContext(scmContext);
    configurator.setLeaseManager(new LeaseManager<>("test-leaseManager", 0));
    scm = HddsTestUtils.getScm(conf, configurator);
    configurator.getLeaseManager().start();

    // Initialize these fields so that the tests can pass.
    ContainerManager mapping = scm.getContainerManager();
    blockManager = (BlockManagerImpl) scm.getScmBlockManager();
    DatanodeCommandHandler handler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, handler);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(pipelineManager, mapping, scmContext,
            configurator.getLeaseManager(), 0);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    replicationConfig = RatisReplicationConfig
        .getInstance(ReplicationFactor.THREE);

    scm.getScmContext().updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
  }

  @AfterEach
  public void cleanup() throws Exception {
    scm.stop();
    scm.join();
    eventQueue.close();
    scmMetadataStore.stop();
  }

  @Test
  public void testAllocateBlock() throws Exception {
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        replicationConfig, OzoneConsts.OZONE, new ExcludeList());
    assertNotNull(block);
  }

  @Test
  public void testAllocateBlockWithExclusion() throws Exception {
    try {
      while (true) {
        pipelineManager.createPipeline(replicationConfig);
      }
    } catch (IOException e) {
    }
    HddsTestUtils.openAllRatisPipelines(pipelineManager);
    ExcludeList excludeList = new ExcludeList();
    excludeList
        .addPipeline(pipelineManager.getPipelines(replicationConfig)
            .get(0).getId());
    AllocatedBlock block = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            excludeList);
    assertNotNull(block);
    for (PipelineID id : excludeList.getPipelineIds()) {
      assertNotEquals(block.getPipeline().getId(), id);
    }

    for (Pipeline pipeline : pipelineManager.getPipelines(replicationConfig)) {
      excludeList.addPipeline(pipeline.getId());
    }
    block = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            excludeList);
    assertNotNull(block);
    assertThat(excludeList.getPipelineIds()).contains(block.getPipeline().getId());
  }

  @Test
  void testAllocateBlockInParallel() throws Exception {
    int threadCount = 20;
    List<ExecutorService> executors = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
    List<CompletableFuture<AllocatedBlock>> futureList =
        new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      final CompletableFuture<AllocatedBlock> future =
          new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        try {
          future.complete(blockManager
              .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                  OzoneConsts.OZONE,
                  new ExcludeList()));
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return future;
      }, executors.get(i));
      futureList.add(future);
    }

    CompletableFuture
        .allOf(futureList.toArray(new CompletableFuture[0]))
        .get();
  }

  @Test
  void testBlockDistribution() throws Exception {
    int threadCount = numContainerPerOwnerInPipeline *
            numContainerPerOwnerInPipeline;
    nodeManager.setNumPipelinePerDatanode(1);
    List<ExecutorService> executors = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);
    Map<Long, List<AllocatedBlock>> allocatedBlockMap =
            new ConcurrentHashMap<>();
    List<CompletableFuture<AllocatedBlock>> futureList =
            new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      final CompletableFuture<AllocatedBlock> future =
              new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        try {
          List<AllocatedBlock> blockList;
          AllocatedBlock block = blockManager
              .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                  OzoneConsts.OZONE,
                  new ExcludeList());
          long containerId = block.getBlockID().getContainerID();
          if (!allocatedBlockMap.containsKey(containerId)) {
            blockList = new ArrayList<>();
          } else {
            blockList = allocatedBlockMap.get(containerId);
          }
          blockList.add(block);
          allocatedBlockMap.put(containerId, blockList);
          future.complete(block);
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return future;
      }, executors.get(i));
      futureList.add(future);
    }
    CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).get();
    Pipeline pipeline = pipelineManager.getPipelines(replicationConfig).get(0);
    int expectedContainers = pipelineManager.openContainerLimit(pipeline, numContainerPerOwnerInPipeline);
    assertEquals(1, pipelineManager.getPipelines(replicationConfig).size());
    assertEquals(expectedContainers, allocatedBlockMap.size());
    assertEquals(expectedContainers, allocatedBlockMap.values().size());
    int floor = threadCount / expectedContainers;
    int ceil = (threadCount + expectedContainers - 1) / expectedContainers;
    allocatedBlockMap.values().forEach(v -> {
      int sz = v.size();
      assertTrue(sz == floor || sz == ceil, "Unexpected blocks per container: " + sz);
    });
  }

  @Test
  void testBlockDistributionWithMultipleDisks() throws Exception {
    int threadCount = numContainerPerOwnerInPipeline *
            numContainerPerOwnerInPipeline;
    nodeManager.setNumHealthyVolumes(numContainerPerOwnerInPipeline);
    nodeManager.setNumPipelinePerDatanode(1);
    List<ExecutorService> executors = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);
    Map<Long, List<AllocatedBlock>> allocatedBlockMap =
            new ConcurrentHashMap<>();
    List<CompletableFuture<AllocatedBlock>> futureList =
            new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      final CompletableFuture<AllocatedBlock> future =
              new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        try {
          List<AllocatedBlock> blockList;
          AllocatedBlock block = blockManager
              .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                  OzoneConsts.OZONE,
                  new ExcludeList());
          long containerId = block.getBlockID().getContainerID();
          if (!allocatedBlockMap.containsKey(containerId)) {
            blockList = new ArrayList<>();
          } else {
            blockList = allocatedBlockMap.get(containerId);
          }
          blockList.add(block);
          allocatedBlockMap.put(containerId, blockList);
          future.complete(block);
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return future;
      }, executors.get(i));
      futureList.add(future);
    }
    CompletableFuture
        .allOf(futureList.toArray(
            new CompletableFuture[futureList.size()])).get();
    assertEquals(1,
        pipelineManager.getPipelines(replicationConfig).size());
    Pipeline pipeline =
        pipelineManager.getPipelines(replicationConfig).get(0);
    // total no of containers to be created will be number of healthy
    // volumes * number of numContainerPerOwnerInPipeline which is equal to
    // the thread count
    assertEquals(threadCount, pipelineManager.getNumberOfContainers(pipeline.getId()));
    assertEquals(threadCount, allocatedBlockMap.size());
    assertEquals(threadCount, allocatedBlockMap.values().size());
    allocatedBlockMap.values().forEach(v -> {
      assertEquals(1, v.size());
    });
  }

  @Test
  void testBlockDistributionWithMultipleRaftLogDisks() throws Exception {
    int threadCount = numContainerPerOwnerInPipeline *
        numContainerPerOwnerInPipeline;
    int numMetaDataVolumes = 2;
    nodeManager.setNumHealthyVolumes(numContainerPerOwnerInPipeline);
    List<ExecutorService> executors = new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executors.add(Executors.newSingleThreadExecutor());
    }
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);
    Map<Long, List<AllocatedBlock>> allocatedBlockMap =
        new ConcurrentHashMap<>();
    List<CompletableFuture<AllocatedBlock>> futureList =
        new ArrayList<>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      final CompletableFuture<AllocatedBlock> future =
          new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        try {
          List<AllocatedBlock> blockList;
          AllocatedBlock block = blockManager
              .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                  OzoneConsts.OZONE,
                  new ExcludeList());
          long containerId = block.getBlockID().getContainerID();
          if (!allocatedBlockMap.containsKey(containerId)) {
            blockList = new ArrayList<>();
          } else {
            blockList = allocatedBlockMap.get(containerId);
          }
          blockList.add(block);
          allocatedBlockMap.put(containerId, blockList);
          future.complete(block);
        } catch (IOException e) {
          future.completeExceptionally(e);
        }
        return future;
      }, executors.get(i));
      futureList.add(future);
    }
    CompletableFuture
        .allOf(futureList.toArray(
            new CompletableFuture[futureList.size()])).get();
    assertEquals(1,
        pipelineManager.getPipelines(replicationConfig).size());
    Pipeline pipeline =
        pipelineManager.getPipelines(replicationConfig).get(0);
    int expectedContainers =
        pipelineManager.openContainerLimit(pipeline, numContainerPerOwnerInPipeline);
    assertEquals(expectedContainers, pipelineManager.getNumberOfContainers(pipeline.getId()));
    assertEquals(expectedContainers, allocatedBlockMap.size());
    assertEquals(expectedContainers, allocatedBlockMap.values().size());
  }

  @Test
  public void testAllocateOversizedBlock() {
    long size = 6 * GB;
    Throwable t = assertThrows(IOException.class, () ->
        blockManager.allocateBlock(size,
            replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
    assertEquals("Unsupported block size: " + size,
        t.getMessage());
  }

  @Test
  public void testAllocateBlockFailureInSafeMode() {
    scm.getScmContext().updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    // Test1: In safe mode expect an SCMException.
    Throwable t = assertThrows(IOException.class, () ->
        blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
            replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
    assertEquals("SafeModePrecheck failed for allocateBlock",
        t.getMessage());
  }

  @Test
  public void testAllocateBlockSucInSafeMode() throws Exception {
    // Test2: Exit safe mode and then try allocateBock again.
    assertNotNull(blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
  }

  @Test
  public void testMultipleBlockAllocation()
      throws IOException, TimeoutException, InterruptedException {

    pipelineManager.createPipeline(replicationConfig);
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);

    AllocatedBlock allocatedBlock = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            new ExcludeList());
    // block should be allocated in different pipelines
    GenericTestUtils.waitFor(() -> {
      try {
        AllocatedBlock block = blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                OzoneConsts.OZONE,
                new ExcludeList());
        return !block.getPipeline().getId()
            .equals(allocatedBlock.getPipeline().getId());
      } catch (IOException e) {
      }
      return false;
    }, 100, 1000);
  }

  private boolean verifyNumberOfContainersInPipelines(
      int numContainersPerPipeline) {
    try {
      for (Pipeline pipeline : pipelineManager
          .getPipelines(replicationConfig)) {
        if (pipelineManager.getNumberOfContainers(pipeline.getId())
            != numContainersPerPipeline) {
          return false;
        }
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  @Test
  public void testMultipleBlockAllocationWithClosedContainer()
      throws IOException, TimeoutException, InterruptedException {
    nodeManager.setNumPipelinePerDatanode(1);
    nodeManager.setNumHealthyVolumes(1);
    // create pipelines
    for (int i = 0;
         i < nodeManager.getNodes(NodeStatus.inServiceHealthy()).size()
             / replicationConfig.getRequiredNodes(); i++) {
      pipelineManager.createPipeline(replicationConfig);
    }
    HddsTestUtils.openAllRatisPipelines(pipelineManager);

    // wait till each pipeline has the configured number of containers.
    // After this each pipeline has numContainerPerOwnerInPipeline containers
    // for each owner
    GenericTestUtils.waitFor(() -> {
      try {
        blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                OzoneConsts.OZONE,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          expectedContainersPerPipeline());
    }, 10, 1000);

    // close all the containers in all the pipelines
    for (Pipeline pipeline : pipelineManager.getPipelines(replicationConfig)) {
      for (ContainerID cid : pipelineManager
          .getContainersInPipeline(pipeline.getId())) {
        eventQueue.fireEvent(SCMEvents.CLOSE_CONTAINER, cid);
      }
    }
    // wait till no containers are left in the pipelines
    GenericTestUtils
        .waitFor(() -> verifyNumberOfContainersInPipelines(0), 10, 5000);

    // allocate block so that each pipeline has the configured number of
    // containers.
    GenericTestUtils.waitFor(() -> {
      try {
        blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                OzoneConsts.OZONE,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          expectedContainersPerPipeline());
    }, 10, 1000);
  }

  @Test
  public void testBlockAllocationWithNoAvailablePipelines()
      throws IOException {
    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      pipelineManager.closePipeline(pipeline.getId());
    }
    assertEquals(0, pipelineManager.getPipelines(replicationConfig).size());
    assertNotNull(blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            new ExcludeList()));
  }

  private class DatanodeCommandHandler implements
      EventHandler<CommandForDatanode> {

    @Override
    public void onMessage(final CommandForDatanode command,
                          final EventPublisher publisher) {
      final SCMCommandProto.Type commandType = command.getCommand().getType();
      if (commandType == SCMCommandProto.Type.createPipelineCommand) {
        CreatePipelineCommand createCommand =
            (CreatePipelineCommand) command.getCommand();
        try {
          pipelineManager.openPipeline(createCommand.getPipelineID());
        } catch (IOException e) {
        }
      }
    }
  }

  private int expectedContainersPerPipeline() {
    Pipeline pipeline = pipelineManager.getPipelines(replicationConfig).get(0);

    return pipelineManager.openContainerLimit(pipeline,
        numContainerPerOwnerInPipeline);
  }
}
