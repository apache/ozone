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

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class tests container commands for reconciliation.
 */
public class TestContainerCommandReconciliation {

  private static MiniOzoneCluster cluster;
  private static OzoneClient rpcClient;
  private static ObjectStore store;
  private static OzoneConfiguration conf;
  private static DNContainerOperationClient dnClient;

  @TempDir
  private static File testDir;

  @BeforeAll
  public static void init() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestContainerCommandReconciliation.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.getBoolean(OZONE_SECURITY_ENABLED_KEY, OZONE_SECURITY_ENABLED_DEFAULT);
    // Disable the container scanner so it does not create merkle tree files that interfere with this test.
    conf.getObject(ContainerScannerConfiguration.class).setEnabled(false);
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

  /**
   * Container checksum trees are only generated for non-open containers.
   * Calling the API on a non-open container should fail.
   */
  @Test
  public void testGetChecksumInfoOpenReplica() throws Exception {
    long containerID = writeDataAndGetContainer(false);
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    StorageContainerException ex = assertThrows(StorageContainerException.class,
        () -> dnClient.getContainerChecksumInfo(containerID, targetDN.getDatanodeDetails()));
    assertEquals(ex.getResult(), ContainerProtos.Result.UNCLOSED_CONTAINER_IO);
  }

  /**
   * Tests reading the container checksum info file from a datanode who does not have a replica for the requested
   * container.
   */
  @Test
  public void testGetChecksumInfoNonexistentReplica() {
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);

    // Find a container ID that does not exist in the cluster. For a small test this should be a good starting
    // point, but modify it just in case.
    long badIDCheck = 1_000_000;
    while (cluster.getStorageContainerManager().getContainerManager()
        .containerExist(ContainerID.valueOf(badIDCheck))) {
      badIDCheck++;
    }

    final long nonexistentContainerID = badIDCheck;
    StorageContainerException ex = assertThrows(StorageContainerException.class,
        () -> dnClient.getContainerChecksumInfo(nonexistentContainerID, targetDN.getDatanodeDetails()));
    assertEquals(ex.getResult(), ContainerProtos.Result.CONTAINER_NOT_FOUND);
  }

  /**
   * Tests reading the container checksum info file from a datanode where the container exists, but the file has not
   * yet been created.
   */
  @Test
  public void testGetChecksumInfoNonexistentFile() throws Exception {
    long containerID = writeDataAndGetContainer(true);
    // Pick a datanode and remove its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = ContainerChecksumTreeManager.getContainerChecksumFile(container.getContainerData());
    // TODO Prior to HDDS-10379 the file will never exist. After that change, the file will always exist after close
    //  and this line needs to be updated to:
    //  assertTrue(treeFile.delete());
    assertFalse(treeFile.exists());

    StorageContainerException ex = assertThrows(StorageContainerException.class, () ->
        dnClient.getContainerChecksumInfo(containerID, targetDN.getDatanodeDetails()));
    assertEquals(ContainerProtos.Result.IO_EXCEPTION, ex.getResult());
    assertTrue(ex.getMessage().contains("(No such file or directory"), ex.getMessage() + " did not contain the expected string");
  }

  /**
   * Tests reading the container checksum info file from a datanode where the datanode fails to read the file from
   * the disk.
   */
  @Test
  public void testGetChecksumInfoServerIOError() throws Exception {
    long containerID = writeDataAndGetContainer(true);
    // Pick a datanode and remove its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    // TODO After HDDS-10379 the file will already exist and we do not need to create one.
    ContainerMerkleTree tree = buildTestTree(conf);
    writeChecksumFileToDatanodes(containerID, tree);
    File treeFile = ContainerChecksumTreeManager.getContainerChecksumFile(container.getContainerData());
    assertTrue(treeFile.exists());
    // Make the server unable to read the file.
    assertTrue(treeFile.setReadable(false));

    StorageContainerException ex = assertThrows(StorageContainerException.class, () ->
        dnClient.getContainerChecksumInfo(containerID, targetDN.getDatanodeDetails()));
    assertEquals(ContainerProtos.Result.IO_EXCEPTION, ex.getResult());
  }

  /**
   * Tests reading the container checksum info file from a datanode where the file is corrupt.
   * The datanode does not deserialize the file before sending it, so there should be no error on the server side
   * when sending the file. The client should raise an error trying to deserialize it.
   */
  @Test
  public void testGetCorruptChecksumInfo() throws Exception {
    long containerID = writeDataAndGetContainer(true);
    // TODO After HDDS-10379 do not need to make this file manually anymore.
    ContainerMerkleTree tree = buildTestTree(conf);
    writeChecksumFileToDatanodes(containerID, tree);

    // Pick a datanode and corrupt its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = ContainerChecksumTreeManager.getContainerChecksumFile(container.getContainerData());
    Files.write(treeFile.toPath(), new byte[]{1, 2, 3},
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    // Reading the file from the replica should fail when the client tries to deserialize it.
    assertThrows(InvalidProtocolBufferException.class, () -> dnClient.getContainerChecksumInfo(containerID,
        targetDN.getDatanodeDetails()));
  }

  @Test
  public void testGetChecksumInfoSuccess() throws Exception {
    long containerID = writeDataAndGetContainer(true);
    // Overwrite the existing tree with a custom one for testing. We will check that it is returned properly from the
    // API.
    ContainerMerkleTree tree = buildTestTree(conf);
    writeChecksumFileToDatanodes(containerID, tree);

    // Verify all the ContainerMerkle Tree matches.
    // This test is expecting Ratis 3 data written on a 3 node cluster, so every node has a replica.
    assertEquals(3, cluster.getHddsDatanodes().size());
    List<DatanodeDetails> datanodeDetails = cluster.getHddsDatanodes().stream()
        .map(HddsDatanodeService::getDatanodeDetails).collect(Collectors.toList());
    for (DatanodeDetails dn: datanodeDetails) {
      ContainerProtos.ContainerChecksumInfo containerChecksumInfo =
          dnClient.getContainerChecksumInfo(containerID, dn);
      assertTreesSortedAndMatch(tree.toProto(), containerChecksumInfo.getContainerMerkleTree());
    }
  }

  private long writeDataAndGetContainer(boolean close) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    byte[] data = "Test content".getBytes(UTF_8);
    // Write Key
    try (OzoneOutputStream os = TestHelper.createKey("testkey", RATIS, THREE, 0, store, volumeName, bucketName)) {
      IOUtils.write(data, os);
    }

    long containerID = bucket.getKey("testkey").getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
    if (close) {
      TestHelper.waitForContainerClose(cluster, containerID);
    }
    return containerID;
  }

  public static void writeChecksumFileToDatanodes(long containerID, ContainerMerkleTree tree) throws Exception {
    // Write Container Merkle Tree
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      KeyValueHandler keyValueHandler =
          (KeyValueHandler) dn.getDatanodeStateMachine().getContainer().getDispatcher()
              .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
      KeyValueContainer keyValueContainer =
          (KeyValueContainer) dn.getDatanodeStateMachine().getContainer().getController()
              .getContainer(containerID);
      keyValueHandler.getChecksumManager().writeContainerDataTree(
          keyValueContainer.getContainerData(), tree);
    }
  }
}
