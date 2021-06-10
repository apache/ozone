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

package org.apache.hadoop.hdds.scm.container;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerV2Impl;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;

import org.apache.commons.lang3.RandomUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.CLOSE_CONTAINER;
import static org.apache.hadoop.hdds.scm.events.SCMEvents.DATANODE_COMMAND;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the closeContainerEventHandler class.
 */
public class TestCloseContainerEventHandler {

  private static OzoneConfiguration configuration;
  private static MockNodeManager nodeManager;
  private static PipelineManagerV2Impl pipelineManager;
  private static ContainerManagerV2 containerManager;
  private static long size;
  private static File testDir;
  private static EventQueue eventQueue;
  private static SCMContext scmContext;
  private static SCMMetadataStore scmMetadataStore;
  private static SCMHAManager scmhaManager;
  private static SequenceIdGenerator sequenceIdGen;

  @BeforeClass
  public static void setUp() throws Exception {
    configuration = SCMTestUtils.getConf();
    size = (long)configuration.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    testDir = GenericTestUtils
        .getTestDir(TestCloseContainerEventHandler.class.getSimpleName());
    configuration
        .set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    configuration.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 16);
    nodeManager = new MockNodeManager(true, 10);
    eventQueue = new EventQueue();
    scmContext = SCMContext.emptyContext();
    scmMetadataStore = new SCMMetadataStoreImpl(configuration);
    scmhaManager = MockSCMHAManager.getInstance(true);
    sequenceIdGen = new SequenceIdGenerator(
        configuration, scmhaManager, scmMetadataStore.getSequenceIdTable());

    SCMServiceManager serviceManager = new SCMServiceManager();

    pipelineManager =
        PipelineManagerV2Impl.newPipelineManager(
            configuration,
            scmhaManager,
            nodeManager,
            scmMetadataStore.getPipelineTable(),
            eventQueue,
            scmContext,
            serviceManager);

    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), configuration, eventQueue);
    pipelineManager.setPipelineProvider(HddsProtos.ReplicationType.RATIS,
        mockRatisProvider);
    containerManager = new ContainerManagerImpl(configuration,
        scmhaManager,
        sequenceIdGen,
        pipelineManager,
        scmMetadataStore.getContainerTable());

    // trigger BackgroundPipelineCreator to take effect.
    serviceManager.notifyEventTriggered(Event.PRE_CHECK_COMPLETED);

    eventQueue.addHandler(CLOSE_CONTAINER,
        new CloseContainerEventHandler(
            pipelineManager, containerManager, scmContext));
    eventQueue.addHandler(DATANODE_COMMAND, nodeManager);
    // Move all pipelines created by background from ALLOCATED to OPEN state
    Thread.sleep(2000);
    TestUtils.openAllRatisPipelines(pipelineManager);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (containerManager != null) {
      containerManager.close();
    }
    if (pipelineManager != null) {
      pipelineManager.close();
    }
    if (scmMetadataStore.getStore() != null) {
      scmMetadataStore.getStore().close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testIfCloseContainerEventHadnlerInvoked() {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    eventQueue.fireEvent(CLOSE_CONTAINER,
        ContainerID.valueOf(Math.abs(RandomUtils.nextInt())));
    eventQueue.processAll(1000);
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Close container Event triggered for container"));
  }

  @Test
  public void testCloseContainerEventWithInvalidContainer() {
    long id = Math.abs(RandomUtils.nextInt());
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    eventQueue.fireEvent(CLOSE_CONTAINER,
        ContainerID.valueOf(id));
    eventQueue.processAll(1000);
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Failed to close the container"));
  }

  @Test
  public void testCloseContainerEventWithValidContainers() throws IOException {
    ContainerInfo container = containerManager
        .allocateContainer(new RatisReplicationConfig(
            ReplicationFactor.ONE), OzoneConsts.OZONE);
    ContainerID id = container.containerID();
    DatanodeDetails datanode = pipelineManager
        .getPipeline(container.getPipelineID()).getFirstNode();
    int closeCount = nodeManager.getCommandCount(datanode);
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    Assert.assertEquals(closeCount + 1,
        nodeManager.getCommandCount(datanode));
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING,
        containerManager.getContainer(id).getState());
  }

  @Test
  public void testCloseContainerEventWithRatis() throws IOException {
    GenericTestUtils.LogCapturer
        .captureLogs(CloseContainerEventHandler.LOG);
    ContainerInfo container = containerManager
        .allocateContainer(new RatisReplicationConfig(
            ReplicationFactor.THREE), OzoneConsts.OZONE);
    ContainerID id = container.containerID();
    int[] closeCount = new int[3];
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    int i = 0;
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      closeCount[i] = nodeManager.getCommandCount(details);
      i++;
    }
    i = 0;
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      Assert.assertEquals(closeCount[i], nodeManager.getCommandCount(details));
      i++;
    }
    eventQueue.fireEvent(CLOSE_CONTAINER, id);
    eventQueue.processAll(1000);
    i = 0;
    // Make sure close is queued for each datanode on the pipeline
    for (DatanodeDetails details : pipelineManager
        .getPipeline(container.getPipelineID()).getNodes()) {
      Assert.assertEquals(closeCount[i] + 1,
          nodeManager.getCommandCount(details));
      Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING,
          containerManager.getContainer(id).getState());
      i++;
    }
  }
}
