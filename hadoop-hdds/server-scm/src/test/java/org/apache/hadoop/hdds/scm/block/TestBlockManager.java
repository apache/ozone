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

package org.apache.hadoop.hdds.scm.block;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
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

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager {
  private StorageContainerManager scm;
  private ContainerManager mapping;
  private MockNodeManager nodeManager;
  private PipelineManagerImpl pipelineManager;
  private BlockManagerImpl blockManager;
  private SCMHAManager scmHAManager;
  private SequenceIdGenerator sequenceIdGen;
  private static final long DEFAULT_BLOCK_SIZE = 128 * MB;
  private EventQueue eventQueue;
  private SCMContext scmContext;
  private SCMServiceManager serviceManager;
  private int numContainerPerOwnerInPipeline;
  private OzoneConfiguration conf;
  private SCMMetadataStore scmMetadataStore;
  private ReplicationConfig replicationConfig;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    conf = SCMTestUtils.getConf();
    numContainerPerOwnerInPipeline = conf.getInt(
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);


    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, tempDir.toString());
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 5,
        TimeUnit.SECONDS);

    // Override the default Node Manager and SCMHAManager
    // in SCM with the Mock one.
    nodeManager = new MockNodeManager(true, 10);
    scmHAManager = SCMHAManagerStub.getInstance(true);

    eventQueue = new EventQueue();
    scmContext = SCMContext.emptyContext();
    serviceManager = new SCMServiceManager();

    scmMetadataStore = new SCMMetadataStoreImpl(conf);
    scmMetadataStore.start(conf);

    sequenceIdGen = new SequenceIdGenerator(
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
                Clock.system(ZoneId.systemDefault())));
    SCMSafeModeManager safeModeManager = new SCMSafeModeManager(conf,
        containerManager.getContainers(), containerManager,
        pipelineManager, eventQueue, serviceManager, scmContext) {
      @Override
      public void emitSafeModeStatus() {
        // skip
      }
    };
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
    mapping = scm.getContainerManager();
    blockManager = (BlockManagerImpl) scm.getScmBlockManager();
    DatanodeCommandHandler handler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, handler);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(pipelineManager, mapping, scmContext,
            configurator.getLeaseManager(), 0);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    replicationConfig = RatisReplicationConfig
        .getInstance(ReplicationFactor.THREE);

    scm.getScmContext().updateSafeModeStatus(new SafeModeStatus(false, true));
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
    Assertions.assertNotNull(block);
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
    Assertions.assertNotNull(block);
    for (PipelineID id : excludeList.getPipelineIds()) {
      Assertions.assertNotEquals(block.getPipeline().getId(), id);
    }

    for (Pipeline pipeline : pipelineManager.getPipelines(replicationConfig)) {
      excludeList.addPipeline(pipeline.getId());
    }
    block = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            excludeList);
    Assertions.assertNotNull(block);
    Assertions.assertTrue(
        excludeList.getPipelineIds().contains(block.getPipeline().getId()));
  }

  @Test
  public void testAllocateBlockInParallel() {
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
    try {
      CompletableFuture
          .allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
          .get();
    } catch (Exception e) {
      Assertions.fail("testAllocateBlockInParallel failed");
    }
  }

  @Test
  public void testBlockDistribution() throws Exception {
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
    try {
      CompletableFuture.allOf(futureList.toArray(
          new CompletableFuture[0])).get();

      Assertions.assertEquals(1,
          pipelineManager.getPipelines(replicationConfig).size());
      Assertions.assertEquals(numContainerPerOwnerInPipeline,
          allocatedBlockMap.size());
      Assertions.assertEquals(numContainerPerOwnerInPipeline,
          allocatedBlockMap.values().size());
      allocatedBlockMap.values().forEach(v -> {
        Assertions.assertEquals(numContainerPerOwnerInPipeline, v.size());
      });
    } catch (Exception e) {
      Assertions.fail("testAllocateBlockInParallel failed");
    }
  }


  @Test
  public void testBlockDistributionWithMultipleDisks() throws Exception {
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
    try {
      CompletableFuture
              .allOf(futureList.toArray(
                      new CompletableFuture[futureList.size()])).get();
      Assertions.assertEquals(1,
          pipelineManager.getPipelines(replicationConfig).size());
      Pipeline pipeline =
          pipelineManager.getPipelines(replicationConfig).get(0);
      // total no of containers to be created will be number of healthy
      // volumes * number of numContainerPerOwnerInPipeline which is equal to
      // the thread count
      Assertions.assertEquals(threadCount, pipelineManager.
              getNumberOfContainers(pipeline.getId()));
      Assertions.assertEquals(threadCount,
              allocatedBlockMap.size());
      Assertions.assertEquals(threadCount, allocatedBlockMap.
              values().size());
      allocatedBlockMap.values().forEach(v -> {
        Assertions.assertEquals(1, v.size());
      });
    } catch (Exception e) {
      Assertions.fail("testAllocateBlockInParallel failed");
    }
  }

  @Test
  public void testBlockDistributionWithMultipleRaftLogDisks() throws Exception {
    int threadCount = numContainerPerOwnerInPipeline *
        numContainerPerOwnerInPipeline;
    int numMetaDataVolumes = 2;
    nodeManager.setNumHealthyVolumes(numContainerPerOwnerInPipeline);
    nodeManager.setNumMetaDataVolumes(numMetaDataVolumes);
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
    try {
      CompletableFuture
          .allOf(futureList.toArray(
              new CompletableFuture[futureList.size()])).get();
      Assertions.assertEquals(1,
          pipelineManager.getPipelines(replicationConfig).size());
      Pipeline pipeline =
          pipelineManager.getPipelines(replicationConfig).get(0);
      // the pipeline per raft log disk config is set to 1 by default
      int numContainers = (int)Math.ceil((double)
              (numContainerPerOwnerInPipeline *
                  numContainerPerOwnerInPipeline) / numMetaDataVolumes);
      Assertions.assertEquals(numContainers, pipelineManager.
          getNumberOfContainers(pipeline.getId()));
      Assertions.assertEquals(numContainers, allocatedBlockMap.size());
      Assertions.assertEquals(numContainers, allocatedBlockMap.values().size());
    } catch (Exception e) {
      Assertions.fail("testAllocateBlockInParallel failed");
    }
  }

  @Test
  public void testAllocateOversizedBlock() {
    long size = 6 * GB;
    Throwable t = Assertions.assertThrows(IOException.class, () ->
        blockManager.allocateBlock(size,
            replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
    Assertions.assertEquals("Unsupported block size: " + size,
        t.getMessage());
  }


  @Test
  public void testAllocateBlockFailureInSafeMode() {
    scm.getScmContext().updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(true, true));
    // Test1: In safe mode expect an SCMException.
    Throwable t = Assertions.assertThrows(IOException.class, () ->
        blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
            replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
    Assertions.assertEquals("SafeModePrecheck failed for allocateBlock",
        t.getMessage());
  }

  @Test
  public void testAllocateBlockSucInSafeMode() throws Exception {
    // Test2: Exit safe mode and then try allocateBock again.
    Assertions.assertNotNull(blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        replicationConfig, OzoneConsts.OZONE, new ExcludeList()));
  }

  @Test
  @Timeout(100)
  public void testMultipleBlockAllocation()
      throws IOException, TimeoutException, InterruptedException {

    pipelineManager.createPipeline(replicationConfig);
    pipelineManager.createPipeline(replicationConfig);
    HddsTestUtils.openAllRatisPipelines(pipelineManager);

    AllocatedBlock allocatedBlock = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig, OzoneConsts.OZONE,
            new ExcludeList());
    // block should be allocated in different pipelines
    await().atMost(Duration.ofSeconds(3))
        .pollInterval(Duration.ofMillis(100))
        .ignoreException(IOException.class)
        .until(() -> {
          AllocatedBlock block = blockManager
              .allocateBlock(DEFAULT_BLOCK_SIZE, replicationConfig,
                  OzoneConsts.OZONE, new ExcludeList());
          return !Objects.equals(block.getPipeline().getId(),
              allocatedBlock.getPipeline().getId());
        });
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
  @Timeout(100)
  public void testMultipleBlockAllocationWithClosedContainer()
      throws IOException, TimeoutException {
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
    await().atMost(Duration.ofSeconds(1))
        .pollInterval(Duration.ofMillis(10))
        .ignoreException(IOException.class)
        .until(() -> {
          blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
              replicationConfig, OzoneConsts.OZONE, new ExcludeList());
          return verifyNumberOfContainersInPipelines(
              numContainerPerOwnerInPipeline);
        });

    // close all the containers in all the pipelines
    for (Pipeline pipeline : pipelineManager.getPipelines(replicationConfig)) {
      for (ContainerID cid : pipelineManager
          .getContainersInPipeline(pipeline.getId())) {
        eventQueue.fireEvent(SCMEvents.CLOSE_CONTAINER, cid);
      }
    }

    // wait till no containers are left in the pipelines
    await().atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> verifyNumberOfContainersInPipelines(0));

    // allocate block so that each pipeline has the configured number of
    // containers.
    await().atMost(Duration.ofSeconds(1))
        .pollInterval(Duration.ofMillis(10))
        .ignoreException(IOException.class)
        .until(() -> {
          blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
              replicationConfig, OzoneConsts.OZONE, new ExcludeList());
          return verifyNumberOfContainersInPipelines(
              numContainerPerOwnerInPipeline);
        });
  }

  @Test
  @Timeout(100)
  public void testBlockAllocationWithNoAvailablePipelines()
      throws IOException {
    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      pipelineManager.closePipeline(pipeline, false);
    }
    Assertions.assertEquals(0,
        pipelineManager.getPipelines(replicationConfig).size());
    Assertions.assertNotNull(blockManager
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
}
