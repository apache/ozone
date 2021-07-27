/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.File;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ha.MockSCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



/**
 * Tests to verify the functionality of ContainerManager.
 */
public class TestContainerManagerImpl {

  private File testDir;
  private DBStore dbStore;
  private ContainerManagerV2 containerManager;
  private SCMHAManager scmhaManager;
  private SequenceIdGenerator sequenceIdGen;

  @Before
  public void setUp() throws Exception {
    final OzoneConfiguration conf = SCMTestUtils.getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = MockSCMHAManager.getInstance(true);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    final PipelineManager pipelineManager = MockPipelineManager.getInstance();
    pipelineManager.createPipeline(new RatisReplicationConfig(
        ReplicationFactor.THREE));
    containerManager = new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore));
  }

  @After
  public void cleanup() throws Exception {
    if(containerManager != null) {
      containerManager.close();
    }

    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testAllocateContainer() throws Exception {
    Assert.assertTrue(
        containerManager.getContainers().isEmpty());
    final ContainerInfo container = containerManager.allocateContainer(
        new RatisReplicationConfig(
            ReplicationFactor.THREE), "admin");
    Assert.assertEquals(1, containerManager.getContainers().size());
    Assert.assertNotNull(containerManager.getContainer(
        container.containerID()));
  }

  @Test
  public void testUpdateContainerState() throws Exception {
    final ContainerInfo container = containerManager.allocateContainer(
        new RatisReplicationConfig(
            ReplicationFactor.THREE), "admin");
    final ContainerID cid = container.containerID();
    Assert.assertEquals(HddsProtos.LifeCycleState.OPEN,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSING,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.QUASI_CLOSE);
    Assert.assertEquals(HddsProtos.LifeCycleState.QUASI_CLOSED,
        containerManager.getContainer(cid).getState());
    containerManager.updateContainerState(cid,
        HddsProtos.LifeCycleEvent.FORCE_CLOSE);
    Assert.assertEquals(HddsProtos.LifeCycleState.CLOSED,
        containerManager.getContainer(cid).getState());
  }

  @Test
  public void testGetContainers() throws Exception{
    Assert.assertTrue(
        containerManager.getContainers().isEmpty());

    ContainerID[] cidArray = new ContainerID[10];
    for(int i = 0; i < 10; i++){
      ContainerInfo container = containerManager.allocateContainer(
          new RatisReplicationConfig(
              ReplicationFactor.THREE), "admin");
      cidArray[i] = container.containerID();
    }

    Assert.assertEquals(10,
        containerManager.getContainers(cidArray[0], 10).size());
    Assert.assertEquals(10,
        containerManager.getContainers(cidArray[0], 100).size());

    containerManager.updateContainerState(cidArray[0],
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assert.assertEquals(9,
        containerManager.getContainers(HddsProtos.LifeCycleState.OPEN).size());
    Assert.assertEquals(1, containerManager
        .getContainers(HddsProtos.LifeCycleState.CLOSING).size());
    containerManager.updateContainerState(cidArray[1],
        HddsProtos.LifeCycleEvent.FINALIZE);
    Assert.assertEquals(8,
        containerManager.getContainers(HddsProtos.LifeCycleState.OPEN).size());
    Assert.assertEquals(2, containerManager
        .getContainers(HddsProtos.LifeCycleState.CLOSING).size());
  }
}
