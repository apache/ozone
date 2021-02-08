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

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for Recon Node Manager.
 */
public class TestReconNodeManager {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private DBStore store;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    store = DBStoreBuilder.createDBStore(conf, new ReconSCMDBDefinition());
  }

  @After
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testReconNodeDB() throws IOException {
    ReconStorageConfig scmStorageConfig = new ReconStorageConfig(conf);
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    Table<UUID, DatanodeDetails> nodeTable =
        ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertTrue(reconNodeManager.getAllNodes().isEmpty());

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    String uuidString = datanodeDetails.getUuidString();

    // Register a random datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNodeByUuid(uuidString),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNodeByUuid(uuidString));

    // If any commands are added to the eventQueue without using the onMessage
    // interface, then they should be filtered out and not returned to the DN
    // when it heartbeats.
    // This command should never be returned by Recon
    reconNodeManager.addDatanodeCommand(datanodeDetails.getUuid(),
        new SetNodeOperationalStateCommand(1234,
        HddsProtos.NodeOperationalState.DECOMMISSIONING, 0));

    // This one should be returned
    reconNodeManager.addDatanodeCommand(datanodeDetails.getUuid(),
        new ReregisterCommand());

    // Upon processing the heartbeat, the illegal command should be filtered out
    List<SCMCommand> returnedCmds =
        reconNodeManager.processHeartbeat(datanodeDetails);
    assertEquals(1, returnedCmds.size());
    assertEquals(SCMCommandProto.Type.reregisterCommand,
        returnedCmds.get(0).getType());

    // Close the DB, and recreate the instance of Recon Node Manager.
    eventQueue.close();
    reconNodeManager.close();
    reconNodeManager = new ReconNodeManager(conf, scmStorageConfig, eventQueue,
        clusterMap, nodeTable);

    // Verify that the node information was persisted and loaded back.
    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(
        reconNodeManager.getNodeByUuid(datanodeDetails.getUuidString()));
  }
}