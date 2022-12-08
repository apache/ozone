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
package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.net.NetConstants.LEAF_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.RACK_SCHEMA;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT_SCHEMA;

import static org.mockito.Mockito.when;

/**
 * Test for scm container placement factory.
 */
public class TestContainerPlacementFactory {
  // network topology cluster
  private NetworkTopology cluster;
  // datanodes array list
  private List<DatanodeDetails> datanodes = new ArrayList<>();
  private List<DatanodeInfo> dnInfos = new ArrayList<>();
  // node storage capacity
  private static final long STORAGE_CAPACITY = 100L;
  // configuration
  private OzoneConfiguration conf;
  // node manager
  private NodeManager nodeManager;

  @BeforeEach
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
  }

  @Test
  public void testRackAwarePolicy() throws IOException {
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackAware.class.getName());
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);

    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());

    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < 15; i++) {
      // Totally 3 racks, each has 5 datanodes
      DatanodeDetails datanodeDetails = MockDatanodeDetails
          .createDatanodeDetails(hostname + i, rack + (i / 5));
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          datanodeDetails, NodeStatus.inServiceHealthy(),
          UpgradeUtils.defaultLayoutVersionProto());

      StorageReportProto storage1 = HddsTestUtils.createStorageReport(
          datanodeInfo.getUuid(), "/data1-" + datanodeInfo.getUuidString(),
          STORAGE_CAPACITY, 0, 100L, null);
      MetadataStorageReportProto metaStorage1 =
          HddsTestUtils.createMetadataStorageReport(
          "/metadata1-" + datanodeInfo.getUuidString(),
          STORAGE_CAPACITY, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));
      datanodeInfo.updateMetaDataStorageReports(
          new ArrayList<>(Arrays.asList(metaStorage1)));

      datanodes.add(datanodeDetails);
      cluster.add(datanodeDetails);
      dnInfos.add(datanodeInfo);
    }

    StorageReportProto storage2 = HddsTestUtils.createStorageReport(
        dnInfos.get(2).getUuid(),
        "/data1-" + dnInfos.get(2).getUuidString(),
        STORAGE_CAPACITY, 90L, 10L, null);
    dnInfos.get(2).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage2)));
    StorageReportProto storage3 = HddsTestUtils.createStorageReport(
        dnInfos.get(3).getUuid(),
        "/data1-" + dnInfos.get(3).getUuidString(),
        STORAGE_CAPACITY, 80L, 20L, null);
    dnInfos.get(3).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage3)));
    StorageReportProto storage4 = HddsTestUtils.createStorageReport(
        dnInfos.get(4).getUuid(),
        "/data1-" + dnInfos.get(4).getUuidString(),
        STORAGE_CAPACITY, 70L, 30L, null);
    dnInfos.get(4).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage4)));

    // create mock node manager
    nodeManager = Mockito.mock(NodeManager.class);
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));
    for (DatanodeInfo dn: dnInfos) {
      when(nodeManager.getNodeByUuid(dn.getUuidString()))
          .thenReturn(dn);
    }

    PlacementPolicy policy = ContainerPlacementPolicyFactory
        .getPolicy(conf, nodeManager, cluster, true,
            SCMContainerPlacementMetrics.create());

    int nodeNum = 3;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15, 15);
    Assertions.assertEquals(nodeNum, datanodeDetails.size());
    Assertions.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assertions.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @Test
  public void testDefaultPolicy() throws IOException {
    PlacementPolicy policy = ContainerPlacementPolicyFactory
        .getPolicy(conf, null, null, true, null);
    Assertions.assertSame(SCMContainerPlacementRandom.class, policy.getClass());
  }

  @Test
  public void testECPolicy() throws IOException {
    PlacementPolicy policy = ContainerPlacementPolicyFactory
        .getECPolicy(conf, null, null, true, null);
    Assertions.assertSame(SCMContainerPlacementRackScatter.class,
        policy.getClass());
  }

  /**
   * A dummy container placement implementation for test.
   */
  public static class DummyImpl implements
          PlacementPolicy<ContainerReplica> {
    @Override
    public List<DatanodeDetails> chooseDatanodes(
        List<DatanodeDetails> usedNodes,
        List<DatanodeDetails> excludedNodes,
        List<DatanodeDetails> favoredNodes,
        int nodesRequired, long metadataSizeRequired, long dataSizeRequired) {
      return null;
    }

    @Override
    public ContainerPlacementStatus
        validateContainerPlacement(List<DatanodeDetails> dns, int replicas) {
      return new ContainerPlacementStatusDefault(1, 1, 1);
    }

    @Override
    public Set<ContainerReplica> replicasToCopyToFixMisreplication(
            Set<ContainerReplica> replicas) {
      return Collections.emptySet();
    }
  }

  @Test
  public void testConstuctorNotFound() {
    // set a placement class which does't have the right constructor implemented
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        DummyImpl.class.getName());

    Assertions.assertThrows(SCMException.class, () ->
        ContainerPlacementPolicyFactory.getPolicy(conf, null, null, true, null)
    );
  }

  @Test
  public void testClassNotImplemented() {
    // set a placement class not implemented
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        "org.apache.hadoop.hdds.scm.container.placement.algorithm.HelloWorld");
    Assertions.assertThrows(RuntimeException.class, () ->
        ContainerPlacementPolicyFactory.getPolicy(conf, null, null, true, null)
    );
  }
}