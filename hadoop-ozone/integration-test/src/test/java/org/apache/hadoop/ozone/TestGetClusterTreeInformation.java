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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.upgrade.UpgradeUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.getRandomPipelineReports;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.createNodeReport;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.createMetadataStorageReport;
import static org.apache.hadoop.hdds.scm.HddsTestUtils.createStorageReport;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.success;

/**
 *
 * This class is to test the serialization/deserialization of cluster tree
 * information from SCM.
 */
@Timeout(300)
public class TestGetClusterTreeInformation {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestGetClusterTreeInformation.class);
  private static int numOfDatanodes = 3;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static OzoneManager om;
  private static SCMNodeManager nodeManager;
  private static OzoneClient client;
  private static ObjectStore store;
  private static OzoneManagerProtocol omClient;

  @BeforeAll
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setNumOfOzoneManagers(3)
        .setNumOfStorageContainerManagers(3)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    om = cluster.getOzoneManager();
    nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    client = cluster.newClient();
    store = client.getObjectStore();
    omClient = store.getClientProxy().getOzoneManagerClient();
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetClusterTreeInformation() throws IOException {
    SCMBlockLocationFailoverProxyProvider failoverProxyProvider =
        new SCMBlockLocationFailoverProxyProvider(conf);
    failoverProxyProvider.changeCurrentProxy(scm.getSCMNodeId());
    ScmBlockLocationProtocolClientSideTranslatorPB scmBlockLocationClient =
        new ScmBlockLocationProtocolClientSideTranslatorPB(
            failoverProxyProvider);

    InnerNode expectedInnerNode = (InnerNode) scm.getClusterMap().getNode(ROOT);
    InnerNode actualInnerNode = scmBlockLocationClient.getNetworkTopology();
    assertEquals(expectedInnerNode, actualInnerNode);
  }

  @Test
  public void testForceFetchClusterTreeInformation() throws IOException {
    omClient.refetchNetworkTopologyTree();

    DatanodeDetails datanode1ToAdd = registerDatanode();
    // OM's copy of network topology does not contain the newly registered DN
    // information at first.
    assertFalse(om.getClusterMap().contains(datanode1ToAdd));

    omClient.refetchNetworkTopologyTree();
    // The API fetches network topology information from SCM on demand,
    // without having to rely on ozone.om.network.topology.refresh.duration.
    // Now, OM's copy of network topology should contain the newly added DN.
    assertTrue(om.getClusterMap().contains(datanode1ToAdd));

    DatanodeDetails datanode2ToAdd = registerDatanode();
    assertFalse(om.getClusterMap().contains(datanode2ToAdd));

    omClient.refetchNetworkTopologyTree();
    assertTrue(om.getClusterMap().contains(datanode2ToAdd));
  }

  private DatanodeDetails registerDatanode() {
    DatanodeDetails details = randomDatanodeDetails();

    StorageReportProto storageReport =
        createStorageReport(details.getUuid(), details.getNetworkFullPath(),
            Long.MAX_VALUE);
    MetadataStorageReportProto metadataStorageReport =
        createMetadataStorageReport(details.getNetworkFullPath(),
            Long.MAX_VALUE);

    LayoutVersionProto layout = UpgradeUtils.defaultLayoutVersionProto();
    RegisteredCommand cmd = nodeManager.register(randomDatanodeDetails(),
        createNodeReport(Arrays.asList(storageReport),
            Arrays.asList(metadataStorageReport)), getRandomPipelineReports(),
        layout);

    assertEquals(success, cmd.getError());
    return cmd.getDatanode();
  }
}
