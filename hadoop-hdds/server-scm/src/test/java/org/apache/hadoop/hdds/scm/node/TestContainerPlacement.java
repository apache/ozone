/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.test.PathUtils;

import org.apache.commons.io.IOUtils;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;

import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterEach;

import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test for different container placement policy.
 */
public class TestContainerPlacement {
  private File testDir;
  private DBStore dbStore;
  private ContainerManager containerManager;
  private SCMHAManager scmhaManager;
  private SequenceIdGenerator sequenceIdGen;
  private OzoneConfiguration conf;
  private PipelineManager pipelineManager;
  private NodeManager nodeManager;

  @BeforeEach
  public void setUp() throws Exception {
    conf = getConf();
    testDir = GenericTestUtils.getTestDir(
        TestContainerPlacement.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    nodeManager = new MockNodeManager(true, 10);
    pipelineManager = new MockPipelineManager(dbStore,
        scmhaManager, nodeManager);
    pipelineManager.createPipeline(RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE));
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
    return new OzoneConfiguration();
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   */

  SCMNodeManager createNodeManager(OzoneConfiguration config) {
    EventQueue eventQueue = new EventQueue();
    eventQueue.addHandler(SCMEvents.NEW_NODE,
        Mockito.mock(NewNodeHandler.class));
    eventQueue.addHandler(SCMEvents.STALE_NODE,
        Mockito.mock(StaleNodeHandler.class));
    eventQueue.addHandler(SCMEvents.DEAD_NODE,
        Mockito.mock(DeadNodeHandler.class));

    SCMStorageConfig storageConfig = Mockito.mock(SCMStorageConfig.class);
    Mockito.when(storageConfig.getClusterID()).thenReturn("cluster1");

    HDDSLayoutVersionManager versionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    Mockito.when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    Mockito.when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    SCMNodeManager scmNodeManager = new SCMNodeManager(config, storageConfig,
        eventQueue, null, SCMContext.emptyContext(), versionManager);
    return scmNodeManager;
  }

  ContainerManager createContainerManager()
      throws IOException {
    return new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore),
        new ContainerReplicaPendingOps(
            Clock.system(ZoneId.systemDefault())));
  }

  /**
   * Test capacity based container placement policy with node reports.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  @Unhealthy
  public void testContainerPlacementCapacity() throws IOException,
      InterruptedException, TimeoutException {
    final int nodeCount = 4;
    final long capacity = 10L * OzoneConsts.GB;
    final long used = 2L * OzoneConsts.GB;
    final long remaining = capacity - used;

    testDir = PathUtils.getTestDir(
        TestContainerPlacement.class);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);

    SCMNodeManager scmNodeManager = createNodeManager(conf);
    containerManager = createContainerManager();
    List<DatanodeDetails> datanodes = HddsTestUtils
        .getListOfRegisteredDatanodeDetails(scmNodeManager, nodeCount);
    XceiverClientManager xceiverClientManager = null;
    LayoutVersionManager versionManager =
        scmNodeManager.getLayoutVersionManager();
    LayoutVersionProto layoutInfo =
        toLayoutVersionProto(versionManager.getMetadataLayoutVersion(),
            versionManager.getSoftwareLayoutVersion());
    try {
      for (DatanodeDetails datanodeDetails : datanodes) {
        scmNodeManager.processHeartbeat(datanodeDetails, layoutInfo);
      }

      //TODO: wait for heartbeat to be processed
      Thread.sleep(4 * 1000);
      assertEquals(nodeCount, scmNodeManager.getNodeCount(null, HEALTHY));
      assertEquals(capacity * nodeCount,
          (long) scmNodeManager.getStats().getCapacity().get());
      assertEquals(used * nodeCount,
          (long) scmNodeManager.getStats().getScmUsed().get());
      assertEquals(remaining * nodeCount,
          (long) scmNodeManager.getStats().getRemaining().get());

      xceiverClientManager = new XceiverClientManager(conf);

      ContainerInfo container = containerManager
          .allocateContainer(
              ReplicationConfig.fromProtoTypeAndFactor(
                  SCMTestUtils.getReplicationType(conf),
                  SCMTestUtils.getReplicationFactor(conf)),
              OzoneConsts.OZONE);
      assertEquals(SCMTestUtils.getReplicationFactor(conf).getNumber(),
          containerManager.getContainerReplicas(
              container.containerID()).size());
    } finally {
      IOUtils.closeQuietly(containerManager);
      IOUtils.closeQuietly(scmNodeManager);
      if (xceiverClientManager != null) {
        xceiverClientManager.close();
      }
      FileUtil.fullyDelete(testDir);
    }
  }
}
