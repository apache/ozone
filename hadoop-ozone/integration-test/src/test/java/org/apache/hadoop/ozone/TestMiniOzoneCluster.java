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

package org.apache.hadoop.ozone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.ozoneimpl.TestOzoneContainer;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.hdds.scm.XceiverClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineChannel;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.test.TestGenericTestUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.junit.Assert.*;

/**
 * Test cases for mini ozone cluster.
 */
public class TestMiniOzoneCluster {

  private static MiniOzoneClassicCluster cluster;
  private static OzoneConfiguration conf;

  private final static File TEST_ROOT = TestGenericTestUtils.getTestDir();
  private final static File WRITE_TMP = new File(TEST_ROOT, "write");
  private final static File READ_TMP = new File(TEST_ROOT, "read");

  @BeforeClass
  public static void setup() {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        TEST_ROOT.toString());
    conf.setBoolean(DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    WRITE_TMP.mkdirs();
    READ_TMP.mkdirs();
    WRITE_TMP.deleteOnExit();
    READ_TMP.deleteOnExit();
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster.close();
    }
  }

  @Test(timeout = 30000)
  public void testStartMultipleDatanodes() throws Exception {
    final int numberOfNodes = 3;
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .numDataNodes(numberOfNodes)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .build();
    List<DataNode> datanodes = cluster.getDataNodes();
    assertEquals(numberOfNodes, datanodes.size());
    for(DataNode dn : datanodes) {
      // Create a single member pipe line
      String containerName = OzoneUtils.getRequestID();
      DatanodeDetails datanodeDetails = null;
      for (ServicePlugin plugin : dn.getPlugins()) {
        if (plugin instanceof HddsDatanodeService) {
          datanodeDetails = ((HddsDatanodeService) plugin).getDatanodeDetails();
        }
      }
      final PipelineChannel pipelineChannel =
          new PipelineChannel(datanodeDetails.getUuidString(),
              HddsProtos.LifeCycleState.OPEN,
              HddsProtos.ReplicationType.STAND_ALONE,
              HddsProtos.ReplicationFactor.ONE, "test");
      pipelineChannel.addMember(datanodeDetails);
      Pipeline pipeline = new Pipeline(containerName, pipelineChannel);

      // Verify client is able to connect to the container
      try (XceiverClient client = new XceiverClient(pipeline, conf)){
        client.connect();
        assertTrue(client.isConnected());
      }
    }
  }

  @Test
  public void testDatanodeIDPersistent() throws Exception {
    // Generate IDs for testing
    DatanodeDetails id1 = TestUtils.getDatanodeDetails();
    DatanodeDetails id2 = TestUtils.getDatanodeDetails();
    DatanodeDetails id3 = TestUtils.getDatanodeDetails();
    id1.setContainerPort(1);
    id2.setContainerPort(2);
    id3.setContainerPort(3);

    // Write a single ID to the file and read it out
    File validIdsFile = new File(WRITE_TMP, "valid-values.id");
    validIdsFile.delete();
    ContainerUtils.writeDatanodeDetailsTo(id1, validIdsFile);
    DatanodeDetails validId = ContainerUtils.readDatanodeDetailsFrom(
        validIdsFile);

    assertEquals(id1, validId);
    assertEquals(id1.getProtoBufMessage(), validId.getProtoBufMessage());

    // Write should fail if unable to create file or directory
    File invalidPath = new File(WRITE_TMP, "an/invalid/path");
    try {
      ContainerUtils.writeDatanodeDetailsTo(id1, invalidPath);
      Assert.fail();
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Read should return an empty value if file doesn't exist
    File nonExistFile = new File(READ_TMP, "non_exist.id");
    nonExistFile.delete();
    try {
      ContainerUtils.readDatanodeDetailsFrom(nonExistFile);
      Assert.fail();
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Read should fail if the file is malformed
    File malformedFile = new File(READ_TMP, "malformed.id");
    createMalformedIDFile(malformedFile);
    try {
      ContainerUtils.readDatanodeDetailsFrom(malformedFile);
      fail("Read a malformed ID file should fail");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  @Test
  public void testContainerRandomPort() throws IOException {
    Configuration ozoneConf = SCMTestUtils.getConf();
    File testDir = PathUtils.getTestDir(TestOzoneContainer.class);
    ozoneConf.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    ozoneConf.set(OZONE_METADATA_DIRS,
        TEST_ROOT.toString());

    // Each instance of SM will create an ozone container
    // that bounds to a random port.
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, true);
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT,
        true);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf)
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getContainerServerPort()));
      assertTrue(ports.add(sm2.getContainer().getContainerServerPort()));
      assertTrue(ports.add(sm3.getContainer().getContainerServerPort()));

      // Assert that ratis is also on a different port.
      assertTrue(ports.add(sm1.getContainer().getRatisContainerServerPort()));
      assertTrue(ports.add(sm2.getContainer().getRatisContainerServerPort()));
      assertTrue(ports.add(sm3.getContainer().getRatisContainerServerPort()));


    }

    // Turn off the random port flag and test again
    ozoneConf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);
    try (
        DatanodeStateMachine sm1 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm2 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf);
        DatanodeStateMachine sm3 = new DatanodeStateMachine(
            TestUtils.getDatanodeDetails(), ozoneConf)
    ) {
      HashSet<Integer> ports = new HashSet<Integer>();
      assertTrue(ports.add(sm1.getContainer().getContainerServerPort()));
      assertFalse(ports.add(sm2.getContainer().getContainerServerPort()));
      assertFalse(ports.add(sm3.getContainer().getContainerServerPort()));
      assertEquals(ports.iterator().next().intValue(),
          conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
              OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT));
    }
  }

  private void createMalformedIDFile(File malformedFile)
      throws IOException{
    malformedFile.delete();
    DatanodeDetails id = TestUtils.getDatanodeDetails();
    ContainerUtils.writeDatanodeDetailsTo(id, malformedFile);

    FileOutputStream out = new FileOutputStream(malformedFile);
    out.write("malformed".getBytes());
    out.close();
  }
}
