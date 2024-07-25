/*
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

package org.apache.hadoop.ozone.dn.checksum;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.checksum.TestContainerMerkleTree.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.TestContainerMerkleTree.buildChunk;

/**
 * This class tests container commands for reconciliation.
 */
public class TestContainerCommandReconciliation {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestContainerCommandReconciliation.class);
  private static MiniOzoneCluster cluster;
  private static OzoneClient rpcClient;
  private static ObjectStore store;
  private static OzoneConfiguration conf;
  private static File testDir;
  private static DNContainerOperationClient dnClient;

  @BeforeAll
  public static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestContainerCommandReconciliation.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.getBoolean(OZONE_SECURITY_ENABLED_KEY,
        OZONE_SECURITY_ENABLED_DEFAULT);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    rpcClient = OzoneClientFactory.getRpcClient(conf);
    store = rpcClient.getObjectStore();
    dnClient = new DNContainerOperationClient(conf, null, null);
  }

  @AfterAll
  public static void stop() throws IOException {
    if (rpcClient != null) {
      rpcClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetContainerMerkleTree() throws Exception {
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();
    final String keyName = UUID.randomUUID().toString();
    byte[] data = "Test content".getBytes(UTF_8);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    // Write Key
    try (OzoneOutputStream os = bucket.createKey(keyName, data.length)) {
      IOUtils.write(data, os);
    }

    // Close container
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainers().get(0);
    closeContainer(container);

    //Write Checksum Data
    ContainerMerkleTree tree = buildTestTree();
    writeChecksumFileToDatanode(container, tree);

    // Verify all the ContainerMerkle Tree matches
    List<DatanodeDetails> datanodeDetails = cluster.getHddsDatanodes().stream()
        .map(HddsDatanodeService::getDatanodeDetails).collect(Collectors.toList());
    for (DatanodeDetails dn: datanodeDetails) {
      ByteString merkleTree = dnClient.getContainerMerkleTree(container.getContainerID(), dn);
      ContainerProtos.ContainerChecksumInfo containerChecksumInfo =
          ContainerProtos.ContainerChecksumInfo.parseFrom(merkleTree);
      assertTreesSortedAndMatch(tree.toProto(), containerChecksumInfo.getContainerMerkleTree());
    }
  }

  public static void writeChecksumFileToDatanode(ContainerInfo container, ContainerMerkleTree tree) throws Exception {
    // Write Container Merkle Tree
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      KeyValueHandler keyValueHandler =
          (KeyValueHandler) dn.getDatanodeStateMachine().getContainer().getDispatcher()
              .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
      KeyValueContainer keyValueContainer =
          (KeyValueContainer) dn.getDatanodeStateMachine().getContainer().getController()
              .getContainer(container.getContainerID());
      keyValueHandler.getChecksumManager().writeContainerDataTree(
          keyValueContainer.getContainerData(), tree);
    }
  }

  public static void closeContainer(ContainerInfo container) throws Exception {
    //Close the container first.
    cluster.getStorageContainerManager().getClientProtocolServer().closeContainer(container.getContainerID());
    GenericTestUtils.waitFor(() -> checkContainerState(container), 100, 50000);
  }

  private static boolean checkContainerState(ContainerInfo container) {
    ContainerInfo containerInfo =  null;
    try {
      containerInfo = cluster.getStorageContainerManager()
          .getContainerInfo(container.getContainerID());
      return containerInfo.getState() == HddsProtos.LifeCycleState.CLOSED;
    } catch (IOException e) {
      LOG.error("Error when getting the container info", e);
    }
    return false;
  }

  public static ContainerMerkleTree buildTestTree() throws Exception {
    final long blockID1 = 1;
    final long blockID2 = 2;
    final long blockID3 = 3;
    ChunkInfo b1c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{1, 2, 3}));
    ChunkInfo b1c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{4, 5, 6}));
    ChunkInfo b2c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{7, 8, 9}));
    ChunkInfo b2c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{12, 11, 10}));
    ChunkInfo b3c1 = buildChunk(0, ByteBuffer.wrap(new byte[]{13, 14, 15}));
    ChunkInfo b3c2 = buildChunk(1, ByteBuffer.wrap(new byte[]{16, 17, 18}));

    ContainerMerkleTree tree = new ContainerMerkleTree();
    tree.addChunks(blockID1, Arrays.asList(b1c1, b1c2));
    tree.addChunks(blockID2, Arrays.asList(b2c1, b2c2));
    tree.addChunks(blockID3, Arrays.asList(b3c1, b3c2));

    return tree;
  }
}
