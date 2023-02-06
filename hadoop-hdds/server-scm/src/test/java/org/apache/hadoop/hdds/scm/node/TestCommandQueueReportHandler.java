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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;

/**
 * Tests for the CommandQueueReportHandler class.
 */
public class TestCommandQueueReportHandler implements EventPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCommandQueueReportHandler.class);
  private CommandQueueReportHandler commandQueueReportHandler;
  private HDDSLayoutVersionManager versionManager;
  private SCMNodeManager nodeManager;


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
    commandQueueReportHandler = new CommandQueueReportHandler(nodeManager);
  }

  @Test
  public void testQueueReportProcessed() throws NodeNotFoundException {
    DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
    nodeManager.register(dn, null, null);

    // Add some queued commands to be sent to the DN on this heartbeat. This
    // means the real queued count will be the value reported plus the commands
    // sent to the DN.
    Map<SCMCommandProto.Type, Integer> commandsToBeSent = new HashMap<>();
    commandsToBeSent.put(SCMCommandProto.Type.valueOf(1), 2);
    commandsToBeSent.put(SCMCommandProto.Type.valueOf(2), 1);

    SCMDatanodeHeartbeatDispatcher.CommandQueueReportFromDatanode
        commandReport = getQueueReport(dn, commandsToBeSent);
    commandQueueReportHandler.onMessage(commandReport, this);

    int commandCount = commandReport.getReport().getCommandCount();
    for (int i = 0; i < commandCount; i++) {
      int storedCount = nodeManager.getNodeQueuedCommandCount(dn,
          commandReport.getReport().getCommand(i));
      int expectedCount = commandReport.getReport().getCount(i);
      // For the first two commands, we added extra commands
      if (i == 0) {
        expectedCount += 2;
      }
      if (i == 1) {
        expectedCount += 1;
      }
      Assertions.assertEquals(expectedCount, storedCount);
      Assertions.assertTrue(storedCount > 0);
    }
  }

  private SCMDatanodeHeartbeatDispatcher.CommandQueueReportFromDatanode
      getQueueReport(DatanodeDetails dn,
          Map<SCMCommandProto.Type, Integer> commandsToBeSent) {
    CommandQueueReportProto.Builder builder =
        CommandQueueReportProto.newBuilder();
    int i = 10;
    for (SCMCommandProto.Type cmd : SCMCommandProto.Type.values()) {
      if (cmd == SCMCommandProto.Type.unknownScmCommand) {
        // Do not include the unknown command type in the message.
        continue;
      }
      builder.addCommand(cmd);
      builder.addCount(i++);
    }
    return new SCMDatanodeHeartbeatDispatcher.CommandQueueReportFromDatanode(
        dn, builder.build(), commandsToBeSent);
  }


  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
  }
}
