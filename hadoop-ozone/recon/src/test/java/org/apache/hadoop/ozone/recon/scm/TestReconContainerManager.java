/**
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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.AbstractOMMetadataManagerTest.getRandomPipeline;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test Recon Container Manager.
 */
public class TestReconContainerManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    scmStorageConfig = new ReconStorageConfig(conf);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap);
    pipelineManager = new ReconPipelineManager(conf, nodeManager, eventQueue);
    containerManager = new ReconContainerManager(conf, pipelineManager);
  }

  @After
  public void tearDown() throws IOException {
    containerManager.close();
    pipelineManager.close();
  }

  @Test
  public void testAddNewContainer() throws IOException {
    ContainerID containerID = new ContainerID(100L);
    Pipeline pipeline = getRandomPipeline();
    pipelineManager.addPipeline(pipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationFactor(ONE)
            .setOwner("test")
            .setState(OPEN)
            .setReplicationType(STAND_ALONE)
            .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);

    assertFalse(containerManager.exists(containerID));

    containerManager.addNewContainer(
        containerID.getId(), containerWithPipeline);

    assertTrue(containerManager.exists(containerID));

    List<ContainerInfo> containers = containerManager.getContainers(OPEN);
    assertEquals(1, containers.size());
    assertTrue(containers.get(0).equals(containerInfo));
    NavigableSet<ContainerID> containersInPipeline =
        pipelineManager.getContainersInPipeline(pipeline.getId());
    assertEquals(1, containersInPipeline.size());
    assertTrue(containersInPipeline.first().equals(containerID));
  }

}