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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.CloseContainerEventHandler;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.SCMPipelineManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.CreatePipelineCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.OzoneConsts.MB;


/**
 * Tests for SCM Block Manager.
 */
public class TestBlockManager {
  private StorageContainerManager scm;
  private SCMContainerManager mapping;
  private MockNodeManager nodeManager;
  private SCMPipelineManager pipelineManager;
  private BlockManagerImpl blockManager;
  private final static long DEFAULT_BLOCK_SIZE = 128 * MB;
  private static HddsProtos.ReplicationFactor factor;
  private static HddsProtos.ReplicationType type;
  private EventQueue eventQueue;
  private int numContainerPerOwnerInPipeline;
  private OzoneConfiguration conf;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    conf = SCMTestUtils.getConf();
    numContainerPerOwnerInPipeline = conf.getInt(
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT_DEFAULT);


    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.newFolder().toString());
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 5,
        TimeUnit.SECONDS);

    // Override the default Node Manager in SCM with this Mock Node Manager.
    nodeManager = new MockNodeManager(true, 10);
    eventQueue = new EventQueue();
    pipelineManager =
        new SCMPipelineManager(conf, nodeManager, eventQueue);
    pipelineManager.allowPipelineCreation();
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf, eventQueue);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    SCMContainerManager containerManager =
        new SCMContainerManager(conf, pipelineManager);
    SCMSafeModeManager safeModeManager = new SCMSafeModeManager(conf,
        containerManager.getContainers(), pipelineManager, eventQueue) {
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
    scm = TestUtils.getScm(conf, configurator);

    // Initialize these fields so that the tests can pass.
    mapping = (SCMContainerManager) scm.getContainerManager();
    blockManager = (BlockManagerImpl) scm.getScmBlockManager();
    DatanodeCommandHandler handler = new DatanodeCommandHandler();
    eventQueue.addHandler(SCMEvents.DATANODE_COMMAND, handler);
    CloseContainerEventHandler closeContainerHandler =
        new CloseContainerEventHandler(pipelineManager, mapping);
    eventQueue.addHandler(SCMEvents.CLOSE_CONTAINER, closeContainerHandler);
    factor = HddsProtos.ReplicationFactor.THREE;
    type = HddsProtos.ReplicationType.RATIS;

    blockManager.handleSafeModeTransition(
        new SCMSafeModeManager.SafeModeStatus(false, false));
  }

  @After
  public void cleanup() {
    scm.stop();
    scm.join();
    eventQueue.close();
  }

  @Test
  public void testAllocateBlock() throws Exception {
    pipelineManager.createPipeline(type, factor);
    TestUtils.openAllRatisPipelines(pipelineManager);
    AllocatedBlock block = blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, OzoneConsts.OZONE, new ExcludeList());
    Assert.assertNotNull(block);
  }

  @Test
  public void testAllocateBlockWithExclusion() throws Exception {
    try {
      while (true) {
        pipelineManager.createPipeline(type, factor);
      }
    } catch (IOException e) {
    }
    TestUtils.openAllRatisPipelines(pipelineManager);
    ExcludeList excludeList = new ExcludeList();
    excludeList
        .addPipeline(pipelineManager.getPipelines(type, factor).get(0).getId());
    AllocatedBlock block = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
            excludeList);
    Assert.assertNotNull(block);
    Assert.assertNotEquals(block.getPipeline().getId(),
        excludeList.getPipelineIds().get(0));

    for (Pipeline pipeline : pipelineManager.getPipelines(type, factor)) {
      excludeList.addPipeline(pipeline.getId());
    }
    block = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
            excludeList);
    Assert.assertNotNull(block);
    Assert.assertTrue(
        excludeList.getPipelineIds().contains(block.getPipeline().getId()));
  }

  @Test
  public void testAllocateBlockInParallel() throws Exception {
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
              .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor,
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
      Assert.fail("testAllocateBlockInParallel failed");
    }
  }

  @Test
  public void testAllocateOversizedBlock() throws Exception {
    long size = 6 * GB;
    thrown.expectMessage("Unsupported block size");
    AllocatedBlock block = blockManager.allocateBlock(size,
        type, factor, OzoneConsts.OZONE, new ExcludeList());
  }


  @Test
  public void testAllocateBlockFailureInSafeMode() throws Exception {
    blockManager.handleSafeModeTransition(
        new SCMSafeModeManager.SafeModeStatus(true, true));
    // Test1: In safe mode expect an SCMException.
    thrown.expectMessage("SafeModePrecheck failed for "
        + "allocateBlock");
    blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, OzoneConsts.OZONE, new ExcludeList());
  }

  @Test
  public void testAllocateBlockSucInSafeMode() throws Exception {
    // Test2: Exit safe mode and then try allocateBock again.
    Assert.assertNotNull(blockManager.allocateBlock(DEFAULT_BLOCK_SIZE,
        type, factor, OzoneConsts.OZONE, new ExcludeList()));
  }

  @Test(timeout = 10000)
  public void testMultipleBlockAllocation()
      throws IOException, TimeoutException, InterruptedException {

    pipelineManager.createPipeline(type, factor);
    pipelineManager.createPipeline(type, factor);
    TestUtils.openAllRatisPipelines(pipelineManager);

    AllocatedBlock allocatedBlock = blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
            new ExcludeList());
    // block should be allocated in different pipelines
    GenericTestUtils.waitFor(() -> {
      try {
        AllocatedBlock block = blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
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
      for (Pipeline pipeline : pipelineManager.getPipelines(type, factor)) {
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

  @Test(timeout = 10000)
  public void testMultipleBlockAllocationWithClosedContainer()
      throws IOException, TimeoutException, InterruptedException {
    // create pipelines
    for (int i = 0;
         i < nodeManager.getNodes(HddsProtos.NodeState.HEALTHY).size() / factor
             .getNumber(); i++) {
      pipelineManager.createPipeline(type, factor);
    }
    TestUtils.openAllRatisPipelines(pipelineManager);

    // wait till each pipeline has the configured number of containers.
    // After this each pipeline has numContainerPerOwnerInPipeline containers
    // for each owner
    GenericTestUtils.waitFor(() -> {
      try {
        blockManager
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          numContainerPerOwnerInPipeline);
    }, 10, 1000);

    // close all the containers in all the pipelines
    for (Pipeline pipeline : pipelineManager.getPipelines(type, factor)) {
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
            .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
                new ExcludeList());
      } catch (IOException e) {
      }
      return verifyNumberOfContainersInPipelines(
          numContainerPerOwnerInPipeline);
    }, 10, 1000);
  }

  @Test(timeout = 10000)
  public void testBlockAllocationWithNoAvailablePipelines()
      throws IOException, TimeoutException, InterruptedException {
    for (Pipeline pipeline : pipelineManager.getPipelines()) {
      pipelineManager.finalizeAndDestroyPipeline(pipeline, false);
    }
    Assert.assertEquals(0, pipelineManager.getPipelines(type, factor).size());
    Assert.assertNotNull(blockManager
        .allocateBlock(DEFAULT_BLOCK_SIZE, type, factor, OzoneConsts.OZONE,
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
