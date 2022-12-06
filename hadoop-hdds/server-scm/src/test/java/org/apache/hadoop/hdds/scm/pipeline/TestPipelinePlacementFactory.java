/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.TestContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackScatter;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;
import static org.mockito.Mockito.when;

/**
 * Test for PipelinePlacementFactory.
 */
public class TestPipelinePlacementFactory {
  private OzoneConfiguration conf;
  private NodeManager nodeManager;
  private NodeManager nodeManagerBase;
  private PipelineStateManager stateManager;
  private NetworkTopologyImpl cluster;
  private final List<DatanodeDetails> datanodes = new ArrayList<>();
  private final List<DatanodeInfo> dnInfos = new ArrayList<>();
  private File testDir;
  private DBStore dbStore;
  private SCMHAManager scmhaManager;

  private static final long STORAGE_CAPACITY = 100L;

  @BeforeEach
  public void setup() {
    //initialize ozone config for tests
    conf = new OzoneConfiguration();
  }

  private void setupRacks(int datanodeCount, int nodesPerRack)
      throws Exception {
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        1, StorageUnit.BYTES);
    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());

    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < datanodeCount; i++) {
      DatanodeDetails datanodeDetails =
          MockDatanodeDetails.createDatanodeDetails(
              hostname + i, rack + (i / nodesPerRack));

      datanodes.add(datanodeDetails);
      cluster.add(datanodeDetails);
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          datanodeDetails, NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageContainerDatanodeProtocolProtos.StorageReportProto storage1 =
          HddsTestUtils.createStorageReport(
          datanodeInfo.getUuid(), "/data1-" + datanodeInfo.getUuidString(),
          STORAGE_CAPACITY, 0, 100L, null);
      StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto
          metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
              "/metadata1-" + datanodeInfo.getUuidString(),
              STORAGE_CAPACITY, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));
      dnInfos.add(datanodeInfo);
    }
    nodeManagerBase = new MockNodeManager(cluster, datanodes,
        false, 10);
    nodeManager = Mockito.spy(nodeManagerBase);
    for (DatanodeInfo dn: dnInfos) {
      when(nodeManager.getNodeByUuid(dn.getUuidString()))
          .thenReturn(dn);
    }

    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    scmhaManager = SCMHAManagerStub.getInstance(true);

    stateManager = PipelineStateManagerImpl.newBuilder()
        .setPipelineStore(SCMDBDefinition.PIPELINES.getTable(dbStore))
        .setRatisServer(scmhaManager.getRatisServer())
        .setNodeManager(nodeManager)
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();
  }

  @Test
  public void testDefaultPolicy() throws IOException {
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(null, null, conf);
    Assertions.assertSame(PipelinePlacementPolicy.class, policy.getClass());
  }

  @Test
  public void testRackScatterPolicy() throws Exception {
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());
    // for this test, rack setup does not matter, just
    // need a non-null NetworkTopologyMap within the nodeManager
    setupRacks(6, 3);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);
    Assertions.assertSame(SCMContainerPlacementRackScatter.class,
        policy.getClass());
  }

  // test default rack aware pipeline provider placement - 3 racks
  // pipeline created with 1 node on one rack and other 2 nodes
  // on separate rack
  @Test
  public void testDefaultPipelineProviderRackPlacement() throws Exception {
    setupRacks(6, 2);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);

    int nodeNum = 3;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15, 15);
    Assertions.assertEquals(nodeNum, datanodeDetails.size());
    Assertions.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  // test rack scatter pipeline provider placement - 3 racks
  // pipeline created with node on each rack
  @Test
  public void testRackScatterPipelineProviderRackPlacement() throws Exception {
    conf.set(OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackScatter.class.getCanonicalName());

    setupRacks(6, 2);
    PlacementPolicy policy = PipelinePlacementPolicyFactory
        .getPolicy(nodeManager, stateManager, conf);

    int nodeNum = 3;
    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    List<DatanodeDetails> favoredNodes = new ArrayList<>();
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(excludedNodes, excludedNodes, favoredNodes,
            nodeNum, 15, 15);
    Assertions.assertEquals(nodeNum, datanodeDetails.size());
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }
}
