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

package org.apache.hadoop.hdds.scm.node;

import static java.util.Collections.emptyList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.MockPipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for different container placement policy.
 */
public class TestContainerPlacement {
  @TempDir
  private File testDir;
  private DBStore dbStore;
  private ContainerManager containerManager;
  private SCMHAManager scmhaManager;
  private SequenceIdGenerator sequenceIdGen;
  private OzoneConfiguration conf;
  private PipelineManager pipelineManager;

  @BeforeEach
  public void setUp() throws Exception {
    conf = getConf();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(conf, SCMDBDefinition.get());
    scmhaManager = SCMHAManagerStub.getInstance(true);
    sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, SCMDBDefinition.SEQUENCE_ID.getTable(dbStore));
    NodeManager nodeManager = new MockNodeManager(true, 10);
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
    eventQueue.addHandler(SCMEvents.NEW_NODE, mock(NewNodeHandler.class));
    eventQueue.addHandler(SCMEvents.STALE_NODE, mock(StaleNodeHandler.class));
    eventQueue.addHandler(SCMEvents.DEAD_NODE, mock(DeadNodeHandler.class));

    SCMStorageConfig storageConfig = mock(SCMStorageConfig.class);
    when(storageConfig.getClusterID()).thenReturn("cluster1");

    HDDSLayoutVersionManager versionManager = mock(HDDSLayoutVersionManager.class);
    when(versionManager.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());
    when(versionManager.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    NetworkTopology networkTopology =
        new NetworkTopologyImpl(NodeSchemaManager.getInstance());
    SCMNodeManager scmNodeManager = new SCMNodeManager(config, storageConfig,
        eventQueue, networkTopology, SCMContext.emptyContext(), versionManager);
    return scmNodeManager;
  }

  ContainerManager createContainerManager()
      throws IOException {
    pipelineManager = spy(pipelineManager);
    doReturn(true).when(pipelineManager).hasEnoughSpace(any(), anyLong());

    return new ContainerManagerImpl(conf,
        scmhaManager, sequenceIdGen, pipelineManager,
        SCMDBDefinition.CONTAINERS.getTable(dbStore),
        new ContainerReplicaPendingOps(
            Clock.system(ZoneId.systemDefault()), null));
  }

  /**
   * Test capacity based container placement policy with node reports.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testContainerPlacementCapacity() throws IOException,
      InterruptedException {
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
    conf.setBoolean(ScmConfigKeys.HDDS_CONTAINER_RATIS_ENABLED_KEY, true);

    SCMNodeManager scmNodeManager = createNodeManager(conf);
    containerManager = createContainerManager();
    List<DatanodeDetails> datanodes = HddsTestUtils
        .getListOfRegisteredDatanodeDetails(scmNodeManager, nodeCount);
    XceiverClientManager xceiverClientManager = null;
    try {
      for (DatanodeDetails datanodeDetails : datanodes) {
        DatanodeID dnId = datanodeDetails.getID();
        DatanodeInfo datanodeInfo = scmNodeManager.getNodeStateManager()
            .getNode(datanodeDetails.getID());
        StorageContainerDatanodeProtocolProtos.StorageReportProto report =
            HddsTestUtils
                .createStorageReport(dnId,
                    testDir.getAbsolutePath() + "/" + dnId, capacity, used,
                    remaining, null);
        StorageContainerDatanodeProtocolProtos.NodeReportProto nodeReportProto =
            HddsTestUtils.createNodeReport(
                Arrays.asList(report), emptyList());
        datanodeInfo.updateStorageReports(
            nodeReportProto.getStorageReportList());
        scmNodeManager.processHeartbeat(datanodeDetails);
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
      assertNotNull(container, "allocateContainer returned null (unexpected in this placement test)");

      int replicaCount = 0;
      for (DatanodeDetails datanodeDetails : datanodes) {
        if (replicaCount ==
            SCMTestUtils.getReplicationFactor(conf).getNumber()) {
          break;
        }
        DatanodeInfo datanodeInfo = scmNodeManager.getNodeStateManager()
            .getNode(datanodeDetails.getID());
        addReplica(container, datanodeInfo);
        replicaCount++;
      }
      assertEquals(SCMTestUtils.getReplicationFactor(conf).getNumber(),
          containerManager.getContainerReplicas(
              container.containerID()).size());
    } catch (NodeNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.closeQuietly(scmNodeManager);
      if (xceiverClientManager != null) {
        xceiverClientManager.close();
      }
    }
  }

  private void addReplica(ContainerInfo cont, DatanodeDetails node) {
    ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(cont.containerID())
        .setContainerState(
            StorageContainerDatanodeProtocolProtos.ContainerReplicaProto
                .State.CLOSED)
        .setDatanodeDetails(node)
        .build();
    containerManager.getContainerStateManager()
        .updateContainerReplica(replica);
  }
}
