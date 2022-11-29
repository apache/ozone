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
package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Node Report Handler.
 */
public class TestNodeReportHandler implements EventPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestNodeReportHandler.class);
  private NodeReportHandler nodeReportHandler;
  private HDDSLayoutVersionManager versionManager;
  private SCMNodeManager nodeManager;
  private String storagePath = GenericTestUtils.getRandomizedTempPath()
      .concat("/data-" + UUID.randomUUID().toString());
  private String metaStoragePath = GenericTestUtils.getRandomizedTempPath()
      .concat("/metadata-" + UUID.randomUUID().toString());

  @BeforeEach
  public void resetEventCollector() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMStorageConfig storageConfig = Mockito.mock(SCMStorageConfig.class);
    Mockito.when(storageConfig.getClusterID()).thenReturn("cluster1");
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);

    this.versionManager =
        Mockito.mock(HDDSLayoutVersionManager.class);
    Mockito.when(versionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    Mockito.when(versionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    nodeManager =
        new SCMNodeManager(conf, storageConfig, new EventQueue(), clusterMap,
            SCMContext.emptyContext(), versionManager);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @Test
  public void testNodeReport() throws IOException {
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    StorageReportProto storageOne = HddsTestUtils
        .createStorageReport(dn.getUuid(), storagePath, 100, 10, 90, null);
    MetadataStorageReportProto metaStorageOne = HddsTestUtils
        .createMetadataStorageReport(metaStoragePath, 100, 10, 90, null);

    SCMNodeMetric nodeMetric = nodeManager.getNodeStat(dn);
    Assertions.assertNull(nodeMetric);

    nodeManager.register(dn, getNodeReport(dn, Arrays.asList(storageOne),
        Arrays.asList(metaStorageOne)).getReport(), null);
    nodeMetric = nodeManager.getNodeStat(dn);

    Assertions.assertTrue(nodeMetric.get().getCapacity().get() == 100);
    Assertions.assertTrue(nodeMetric.get().getRemaining().get() == 90);
    Assertions.assertTrue(nodeMetric.get().getScmUsed().get() == 10);

    StorageReportProto storageTwo = HddsTestUtils
        .createStorageReport(dn.getUuid(), storagePath, 100, 10, 90, null);
    nodeReportHandler.onMessage(
        getNodeReport(dn, Arrays.asList(storageOne, storageTwo),
            Arrays.asList(metaStorageOne)), this);
    nodeMetric = nodeManager.getNodeStat(dn);

    Assertions.assertTrue(nodeMetric.get().getCapacity().get() == 200);
    Assertions.assertTrue(nodeMetric.get().getRemaining().get() == 180);
    Assertions.assertTrue(nodeMetric.get().getScmUsed().get() == 20);

  }

  private NodeReportFromDatanode getNodeReport(DatanodeDetails dn,
      List<StorageReportProto> reports,
      List<MetadataStorageReportProto> metaReports) {
    NodeReportProto nodeReportProto =
        HddsTestUtils.createNodeReport(reports, metaReports);
    return new NodeReportFromDatanode(dn, nodeReportProto);
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
  }
}
