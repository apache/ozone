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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageSize;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test cases for mini ozone cluster.
 */
public class TestMiniOzoneCluster {

  private MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeAll
  static void setup(@TempDir File testDir) {
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setBoolean(HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    conf.set(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "1s");
  }

  @AfterEach
  public void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testStartMultipleDatanodes() throws Exception {
    final int numberOfNodes = 3;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numberOfNodes)
        .build();
    cluster.waitForClusterToBeReady();
    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
    assertEquals(numberOfNodes, datanodes.size());
    for (HddsDatanodeService dn : datanodes) {
      // Create a single member pipe line
      List<DatanodeDetails> dns = new ArrayList<>();
      dns.add(dn.getDatanodeDetails());
      Pipeline pipeline = Pipeline.newBuilder()
          .setState(Pipeline.PipelineState.OPEN)
          .setId(PipelineID.randomId())
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(
              ReplicationFactor.ONE))
          .setNodes(dns)
          .build();

      // Verify client is able to connect to the container
      try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)) {
        client.connect();
        assertTrue(client.isConnected(pipeline.getFirstNode()));
      }
    }
  }

  @Test
  void testContainerRandomPort(@TempDir File tempDir) throws IOException {
    OzoneConfiguration ozoneConf = SCMTestUtils.getConf(tempDir);

    // Each instance of SM will create an ozone container
    // that bounds to a random port.
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT, true);
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);
    ozoneConf.setBoolean(
        OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_RANDOM_PORT, true);
    List<DatanodeStateMachine> stateMachines = new ArrayList<>();
    try {

      for (int i = 0; i < 3; i++) {
        stateMachines.add(new DatanodeStateMachine(
            randomDatanodeDetails(), ozoneConf));
      }

      //we need to start all the servers to get the fix ports
      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.getContainer().getReadChannel().start();
        dsm.getContainer().getWriteChannel().start();

      }

      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.getContainer().getWriteChannel().stop();
        dsm.getContainer().getReadChannel().stop();

      }

      //after the start the real port numbers should be available AND unique
      HashSet<Integer> ports = new HashSet<Integer>();
      for (DatanodeStateMachine dsm : stateMachines) {
        int readPort = dsm.getContainer().getReadChannel().getIPCPort();

        assertNotEquals(0, readPort,
            "Port number of the service is not updated");

        assertTrue(ports.add(readPort),
            "Port of datanode service is conflicted with other server.");

        int writePort = dsm.getContainer().getWriteChannel().getIPCPort();

        assertNotEquals(0, writePort,
            "Port number of the service is not updated");
        assertTrue(ports.add(writePort),
            "Port of datanode service is conflicted with other server.");
      }

    } finally {
      for (DatanodeStateMachine dsm : stateMachines) {
        dsm.close();
      }
    }

    // Turn off the random port flag and test again
    ozoneConf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_IPC_RANDOM_PORT, false);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            randomDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            randomDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            randomDatanodeDetails(), ozoneConf);
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getReadChannel().getIPCPort()));
      assertFalse(ports.add(sm2.getContainer().getReadChannel().getIPCPort()));
      assertFalse(ports.add(sm3.getContainer().getReadChannel().getIPCPort()));
      assertEquals(ports.iterator().next().intValue(),
          conf.getInt(OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT,
              OzoneConfigKeys.HDDS_CONTAINER_IPC_PORT_DEFAULT));
    }
  }

  @Test
  public void testKeepPortsWhenRestartDN() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    DatanodeDetails before =
        cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    cluster.restartHddsDatanode(0, true);
    DatanodeDetails after =
        cluster.getHddsDatanodes().get(0).getDatanodeDetails();
    for (Port.Name name : Port.Name.ALL_PORTS) {
      assertEquals(before.getPort(name), after.getPort(name));
    }
  }

  /**
   * Test that a DN can register with SCM even if it was started before the SCM.
   * @throws Exception
   */
  @Test
  public void testDNstartAfterSCM() throws Exception {
    // Start a cluster with 3 DN
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    // Stop the SCM
    StorageContainerManager scm = cluster.getStorageContainerManager();
    scm.stop();

    // Restart DN
    cluster.restartHddsDatanode(0, false);

    // DN should be in GETVERSION state till the SCM is restarted.
    // Check DN endpoint state for 20 seconds
    DatanodeStateMachine dnStateMachine = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine();
    for (int i = 0; i < 20; i++) {
      for (EndpointStateMachine endpoint :
          dnStateMachine.getConnectionManager().getValues()) {
        assertEquals(
            EndpointStateMachine.EndPointStates.GETVERSION,
            endpoint.getState());
      }
    }

    // DN should successfully register with the SCM after SCM is restarted.
    // Restart the SCM
    cluster.restartStorageContainerManager(true);
    // Wait for DN to register
    cluster.waitForClusterToBeReady();
    // DN should be in HEARTBEAT state after registering with the SCM
    for (EndpointStateMachine endpoint :
        dnStateMachine.getConnectionManager().getValues()) {
      assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          endpoint.getState());
    }
  }

  /**
   * Test that multiple datanode directories are created in MiniOzoneCluster.
   * @throws Exception
   */
  @Test
  public void testMultipleDataDirs() throws Exception {
    // Start a cluster with 3 DN and configure reserved space in each DN
    String reservedSpace = "1B";
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setNumDataVolumes(3)
            .setReservedSpace(reservedSpace)
            .build())
        .build();
    cluster.waitForClusterToBeReady();

    final String name = MiniOzoneClusterImpl.class.getSimpleName()
        + "-" + cluster.getClusterId();
    assertEquals(name, cluster.getName());

    final String baseDir = MiniOzoneCluster.Builder.getTempPath(name);
    assertEquals(baseDir, cluster.getBaseDir());


    List<StorageVolume> volumeList = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer().getVolumeSet()
        .getVolumesList();

    assertEquals(3, volumeList.size());

    volumeList.forEach(storageVolume -> assertEquals(
            (long) StorageSize.parse(reservedSpace).getValue(),
            storageVolume.getVolumeUsage().getReservedInBytes()));
  }

}
