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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.protocol.commands.SetNodeOperationalStateCommand;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for Recon Node Manager.
 */
public class TestReconNodeManager {

  @TempDir
  private Path temporaryFolder;

  private OzoneConfiguration conf;
  private DBStore store;
  private HDDSLayoutVersionManager versionManager;
  private ReconContext reconContext;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, temporaryFolder.toAbsolutePath().toString());
    conf.set(OZONE_SCM_NAMES, "localhost");
    ReconUtils reconUtils = new ReconUtils();
    ReconStorageConfig reconStorageConfig = new ReconStorageConfig(conf, reconUtils);
    versionManager = new HDDSLayoutVersionManager(
        reconStorageConfig.getLayoutVersion());
    store = DBStoreBuilder.createDBStore(conf, ReconSCMDBDefinition.get());
    reconContext = new ReconContext(conf, reconUtils);
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testReconNodeManagerInitWithInvalidNetworkTopology() throws IOException {
    ReconUtils reconUtils = new ReconUtils();
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, reconUtils);
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    final Table<DatanodeID, DatanodeDetails> nodeTable = ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager, reconContext);
    assertThat(reconNodeManager.getAllNodes()).isEmpty();

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    // Updating the node's topology depth to make it invalid.
    datanodeDetails.setNetworkLocation("/default-rack/xyz/");

    // Register a random datanode.
    RegisteredCommand register = reconNodeManager.register(datanodeDetails, null, null);
    assertNotNull(register);
    assertEquals(StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted,
        register.getError());
    assertEquals(reconContext.getClusterId(), register.getClusterID());
    assertFalse(reconContext.isHealthy().get());
    assertTrue(reconContext.getErrors().get(0).equals(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY));

    assertEquals(0, reconNodeManager.getAllNodes().size());
    assertNull(reconNodeManager.getNode(datanodeDetails.getID()));
  }

  @Test
  public void testReconNodeDB() throws IOException, NodeNotFoundException {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    final Table<DatanodeID, DatanodeDetails> nodeTable = ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager, reconContext);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertThat(reconNodeManager.getAllNodes()).isEmpty();

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    final DatanodeID datanodeID = datanodeDetails.getID();

    // Register a random datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNode(datanodeID),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNode(datanodeID));

    // If any commands are added to the eventQueue without using the onMessage
    // interface, then they should be filtered out and not returned to the DN
    // when it heartbeats.
    // This command should never be returned by Recon
    reconNodeManager.addDatanodeCommand(datanodeDetails.getID(),
        new SetNodeOperationalStateCommand(1234,
        DECOMMISSIONING, 0));

    // This one should be returned
    reconNodeManager.addDatanodeCommand(datanodeDetails.getID(),
        new ReregisterCommand());

    // OperationalState sanity check
    final DatanodeDetails dnDetails = reconNodeManager.getNode(datanodeID);
    assertEquals(HddsProtos.NodeOperationalState.IN_SERVICE,
        dnDetails.getPersistedOpState());
    assertEquals(dnDetails.getPersistedOpState(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOperationalState());
    assertEquals(dnDetails.getPersistedOpStateExpiryEpochSec(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOpStateExpiryEpochSeconds());

    // Upon processing the heartbeat, the illegal command should be filtered out
    List<SCMCommand<?>> returnedCmds =
        reconNodeManager.processHeartbeat(datanodeDetails);
    assertEquals(1, returnedCmds.size());
    assertEquals(SCMCommandProto.Type.reregisterCommand,
        returnedCmds.get(0).getType());

    // Now feed a DECOMMISSIONED heartbeat of the same DN
    datanodeDetails.setPersistedOpState(
        HddsProtos.NodeOperationalState.DECOMMISSIONED);
    datanodeDetails.setPersistedOpStateExpiryEpochSec(12345L);
    reconNodeManager.processHeartbeat(datanodeDetails);
    // Check both persistedOpState and NodeStatus#operationalState
    assertEquals(HddsProtos.NodeOperationalState.DECOMMISSIONED,
        dnDetails.getPersistedOpState());
    assertEquals(dnDetails.getPersistedOpState(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOperationalState());
    assertEquals(12345L, dnDetails.getPersistedOpStateExpiryEpochSec());
    assertEquals(dnDetails.getPersistedOpStateExpiryEpochSec(),
        reconNodeManager.getNodeStatus(dnDetails)
            .getOpStateExpiryEpochSeconds());

    // Close the DB, and recreate the instance of Recon Node Manager.
    eventQueue.close();
    reconNodeManager.close();
    reconNodeManager = new ReconNodeManager(conf, scmStorageConfig, eventQueue,
        clusterMap, nodeTable, versionManager, reconContext);

    // Verify that the node information was persisted and loaded back.
    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNode(datanodeDetails.getID()));
  }

  @Test
  public void testUpdateNodeOperationalStateFromScm() throws Exception {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    final Table<DatanodeID, DatanodeDetails> nodeTable = ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager, reconContext);


    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    HddsProtos.Node node = mock(HddsProtos.Node.class);

    assertThrows(NodeNotFoundException.class,
        () -> reconNodeManager
            .updateNodeOperationalStateFromScm(node, datanodeDetails));

    reconNodeManager.register(datanodeDetails, null, null);
    assertEquals(IN_SERVICE, reconNodeManager
        .getNode(datanodeDetails.getID()).getPersistedOpState());

    when(node.getNodeOperationalStates(eq(0)))
        .thenReturn(DECOMMISSIONING);
    reconNodeManager.updateNodeOperationalStateFromScm(node, datanodeDetails);
    assertEquals(DECOMMISSIONING, reconNodeManager
        .getNode(datanodeDetails.getID()).getPersistedOpState());
    List<DatanodeDetails> nodes =
        reconNodeManager.getNodes(DECOMMISSIONING, null);
    assertEquals(1, nodes.size());
    assertEquals(datanodeDetails.getUuid(), nodes.get(0).getUuid());
  }

  @Test
  public void testDatanodeUpdate() throws IOException {
    ReconStorageConfig scmStorageConfig =
        new ReconStorageConfig(conf, new ReconUtils());
    EventQueue eventQueue = new EventQueue();
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    final Table<DatanodeID, DatanodeDetails> nodeTable = ReconSCMDBDefinition.NODES.getTable(store);
    ReconNodeManager reconNodeManager = new ReconNodeManager(conf,
        scmStorageConfig, eventQueue, clusterMap, nodeTable, versionManager, reconContext);
    ReconNewNodeHandler reconNewNodeHandler =
        new ReconNewNodeHandler(reconNodeManager);
    assertThat(reconNodeManager.getAllNodes()).isEmpty();

    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    datanodeDetails.setHostName("hostname1");
    final DatanodeID datanodeID = datanodeDetails.getID();

    // Register "hostname1" datanode.
    reconNodeManager.register(datanodeDetails, null, null);
    reconNewNodeHandler.onMessage(reconNodeManager.getNode(datanodeID),
        null);

    assertEquals(1, reconNodeManager.getAllNodes().size());
    assertNotNull(reconNodeManager.getNode(datanodeID));
    assertEquals("hostname1",
        reconNodeManager.getNode(datanodeID).getHostName());

    datanodeDetails.setHostName("hostname2");
    // Upon processing the heartbeat, the illegal command should be filtered out
    List<SCMCommand<?>> returnedCmds =
        reconNodeManager.processHeartbeat(datanodeDetails);
    assertEquals(1, returnedCmds.size());
    assertEquals(SCMCommandProto.Type.reregisterCommand,
        returnedCmds.get(0).getType());

  }
}
