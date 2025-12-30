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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the statics in NodeManager.
 */
public class TestStatisticsUpdate {

  private NodeManager nodeManager;
  private NodeReportHandler nodeReportHandler;

  @BeforeEach
  void setup(@TempDir File testDir)
      throws IOException, AuthenticationException {
    final OzoneConfiguration conf = SCMTestUtils.getConf(testDir);
    conf.set(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "100ms");
    conf.set(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, "50ms");
    conf.set(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "1s");
    conf.set(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, "2s");
    final EventQueue eventQueue = new EventQueue();
    final StorageContainerManager scm = HddsTestUtils.getScm(conf);
    nodeManager = scm.getScmNodeManager();
    final DeadNodeHandler deadNodeHandler = new DeadNodeHandler(
        nodeManager, mock(PipelineManager.class),
        scm.getContainerManager());
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @Test
  public void testStatisticsUpdate() throws Exception {
    //GIVEN
    DatanodeDetails datanode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode2 = MockDatanodeDetails.randomDatanodeDetails();

    StorageReportProto storageOne = HddsTestUtils.createStorageReport(
        datanode1.getID(), datanode1.getUuidString(), 100, 10, 90, null);
    StorageReportProto storageTwo = HddsTestUtils.createStorageReport(
        datanode2.getID(), datanode2.getUuidString(), 200, 20, 180, null);

    nodeManager.register(datanode1,
        HddsTestUtils.createNodeReport(Arrays.asList(storageOne),
            Collections.emptyList()), null);
    nodeManager.register(datanode2,
        HddsTestUtils.createNodeReport(Arrays.asList(storageTwo),
            Collections.emptyList()), null);

    NodeReportProto nodeReportProto1 = HddsTestUtils.createNodeReport(
        Arrays.asList(storageOne), Collections.emptyList());
    NodeReportProto nodeReportProto2 = HddsTestUtils.createNodeReport(
        Arrays.asList(storageTwo), Collections.emptyList());

    nodeReportHandler.onMessage(
        new NodeReportFromDatanode(datanode1, nodeReportProto1),
        mock(EventPublisher.class));
    nodeReportHandler.onMessage(
        new NodeReportFromDatanode(datanode2, nodeReportProto2),
        mock(EventPublisher.class));

    SCMNodeStat stat = nodeManager.getStats();
    assertEquals(300L, stat.getCapacity().get());
    assertEquals(270L, stat.getRemaining().get());
    assertEquals(30L, stat.getScmUsed().get());

    SCMNodeMetric nodeStat = nodeManager.getNodeStat(datanode1);
    assertEquals(100L, nodeStat.get().getCapacity().get());
    assertEquals(90L, nodeStat.get().getRemaining().get());
    assertEquals(10L, nodeStat.get().getScmUsed().get());

    //TODO: Support logic to mark a node as dead in NodeManager.

    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    Thread.sleep(1000);
    nodeManager.processHeartbeat(datanode2);
    //THEN statistics in SCM should changed.
    stat = nodeManager.getStats();
    assertEquals(200L, stat.getCapacity().get());
    assertEquals(180L, stat.getRemaining().get());
    assertEquals(20L, stat.getScmUsed().get());
  }

}
