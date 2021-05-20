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
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.net.NodeSchema;
import org.apache.hadoop.hdds.scm.net.NodeSchemaManager;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
  private List<DatanodeInfo> datanodes = new ArrayList<>();
  // node storage capacity
  private static final long STORAGE_CAPACITY = 100L;
  // configuration
  private OzoneConfiguration conf;
  // node manager
  private NodeManager nodeManager;

  @Before
  public void setup() {
    //initialize network topology instance
    conf = new OzoneConfiguration();
  }

  @Test
  public void testRackAwarePolicy() throws IOException {
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementRackAware.class.getName());

    NodeSchema[] schemas = new NodeSchema[]
        {ROOT_SCHEMA, RACK_SCHEMA, LEAF_SCHEMA};
    NodeSchemaManager.getInstance().init(schemas, true);
    cluster = new NetworkTopologyImpl(NodeSchemaManager.getInstance());

    // build datanodes, and network topology
    String rack = "/rack";
    String hostname = "node";
    for (int i = 0; i < 15; i++) {
      // Totally 3 racks, each has 5 datanodes
      DatanodeInfo datanodeInfo = new DatanodeInfo(
          MockDatanodeDetails.createDatanodeDetails(
          hostname + i, rack + (i / 5)), NodeStatus.inServiceHealthy());

      StorageReportProto storage1 = TestUtils.createStorageReport(
          datanodeInfo.getUuid(), "/data1-" + datanodeInfo.getUuidString(),
          STORAGE_CAPACITY, 0, 100L, null);
      datanodeInfo.updateStorageReports(
          new ArrayList<>(Arrays.asList(storage1)));

      datanodes.add(datanodeInfo);
      cluster.add(datanodeInfo);
    }

    StorageReportProto storage2 = TestUtils.createStorageReport(
        datanodes.get(2).getUuid(),
        "/data1-" + datanodes.get(2).getUuidString(),
        STORAGE_CAPACITY, 90L, 10L, null);
    datanodes.get(2).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage2)));
    StorageReportProto storage3 = TestUtils.createStorageReport(
        datanodes.get(3).getUuid(),
        "/data1-" + datanodes.get(3).getUuidString(),
        STORAGE_CAPACITY, 80L, 20L, null);
    datanodes.get(3).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage3)));
    StorageReportProto storage4 = TestUtils.createStorageReport(
        datanodes.get(4).getUuid(),
        "/data1-" + datanodes.get(4).getUuidString(),
        STORAGE_CAPACITY, 70L, 30L, null);
    datanodes.get(4).updateStorageReports(
        new ArrayList<>(Arrays.asList(storage4)));

    // create mock node manager
    nodeManager = Mockito.mock(NodeManager.class);
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(new ArrayList<>(datanodes));

    PlacementPolicy policy = ContainerPlacementPolicyFactory
        .getPolicy(conf, nodeManager, cluster, true,
            SCMContainerPlacementMetrics.create());

    int nodeNum = 3;
    List<DatanodeDetails> datanodeDetails =
        policy.chooseDatanodes(null, null, nodeNum, 15);
    Assert.assertEquals(nodeNum, datanodeDetails.size());
    Assert.assertTrue(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(1)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(0),
        datanodeDetails.get(2)));
    Assert.assertFalse(cluster.isSameParent(datanodeDetails.get(1),
        datanodeDetails.get(2)));
  }

  @Test
  public void testDefaultPolicy() throws IOException {
    PlacementPolicy policy = ContainerPlacementPolicyFactory
        .getPolicy(conf, null, null, true, null);
    Assert.assertSame(SCMContainerPlacementRandom.class, policy.getClass());
  }

  /**
   * A dummy container placement implementation for test.
   */
  public static class DummyImpl implements PlacementPolicy {
    @Override
    public List<DatanodeDetails> chooseDatanodes(
        List<DatanodeDetails> excludedNodes, List<DatanodeDetails> favoredNodes,
        int nodesRequired, long sizeRequired) {
      return null;
    }

    @Override
    public ContainerPlacementStatus
        validateContainerPlacement(List<DatanodeDetails> dns, int replicas) {
      return new ContainerPlacementStatusDefault(1, 1, 1);
    }
  }

  @Test(expected = SCMException.class)
  public void testConstuctorNotFound() throws SCMException {
    // set a placement class which does't have the right constructor implemented
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        DummyImpl.class.getName());
    ContainerPlacementPolicyFactory.getPolicy(conf, null, null, true, null);
  }

  @Test(expected = RuntimeException.class)
  public void testClassNotImplemented() throws SCMException {
    // set a placement class not implemented
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        "org.apache.hadoop.hdds.scm.container.placement.algorithm.HelloWorld");
    ContainerPlacementPolicyFactory.getPolicy(conf, null, null, true, null);
  }
}