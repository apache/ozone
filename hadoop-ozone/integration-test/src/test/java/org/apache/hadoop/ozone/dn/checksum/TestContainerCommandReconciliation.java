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

package org.apache.hadoop.ozone.dn.checksum;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_CHECK_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager.getContainerChecksumFile;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.assertTreesSortedAndMatch;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.buildTestTree;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.readChecksumFile;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_REMOVER_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.DELEGATION_TOKEN_MAX_LIFETIME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeWriter;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.TestContainerCorruptions;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests container commands for reconciliation.
 */
public class TestContainerCommandReconciliation {

  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient rpcClient;
  private static ObjectStore store;
  private static OzoneConfiguration conf;
  private static DNContainerOperationClient dnClient;
  private static final String KEY_NAME = "testkey";
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerCommandReconciliation.class);
  private static final String TEST_SCAN = "Test Scan";

  @TempDir
  private static File testDir;
  @TempDir
  private static File workDir;
  private static MiniKdc miniKdc;
  private static File ozoneKeytab;
  private static File spnegoKeytab;
  private static File testUserKeytab;
  private static String testUserPrincipal;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    conf.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, 128 * 1024, StorageUnit.BYTES);
    conf.setStorageSize(OZONE_SCM_BLOCK_SIZE,  512 * 1024, StorageUnit.BYTES);
    // Support restarting datanodes and SCM in a rolling fashion to test checksum reporting after restart.
    // Datanodes need to heartbeat more frequently, because they will not know that SCM was restarted until they
    // heartbeat and SCM indicates they need to re-register.
    conf.set(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, "200ms");
    conf.set(HDDS_HEARTBEAT_INTERVAL, "1s");
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "3s");
    conf.set(OZONE_SCM_DEADNODE_INTERVAL, "6s");
    conf.set(HDDS_NODE_REPORT_INTERVAL, "5s");
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "5s");

    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
    setSecretKeysConfig();
    startCluster();
  }

  @AfterAll
  public static void stop() throws IOException {
    if (rpcClient != null) {
      rpcClient.close();
    }

    if (dnClient != null) {
      dnClient.close();
    }

    if (miniKdc != null) {
      miniKdc.stop();
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
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(false, volume, bucket);
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
   * Tests container checksum file creation if it doesn't exist during getContainerChecksumInfo call.
   */
  @Test
  public void testMerkleTreeCreationDuringGetChecksumInfo() throws Exception {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);
    // Pick a datanode and remove its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = getContainerChecksumFile(container.getContainerData());
    // Closing the container should have generated the tree file.
    ContainerProtos.ContainerChecksumInfo srcChecksumInfo = ContainerChecksumTreeManager.readChecksumInfo(
        container.getContainerData());
    assertTrue(treeFile.exists());
    assertTrue(treeFile.delete());

    ContainerProtos.ContainerChecksumInfo destChecksumInfo = dnClient.getContainerChecksumInfo(
        containerID, targetDN.getDatanodeDetails());
    assertNotNull(destChecksumInfo);
    assertTreesSortedAndMatch(srcChecksumInfo.getContainerMerkleTree(), destChecksumInfo.getContainerMerkleTree());
  }

  /**
   * Tests reading the container checksum info file from a datanode where there's an IO error 
   * that's not related to file not found (e.g., permission error). Such errors should not 
   * trigger fallback to building from metadata.
   */
  @Test
  public void testGetChecksumInfoIOError() throws Exception {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);
    // Pick a datanode and make its checksum file unreadable to simulate permission error.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = getContainerChecksumFile(container.getContainerData());
    assertTrue(treeFile.exists());
    // Make the server unable to read the file (permission error, not file not found).
    assertTrue(treeFile.setReadable(false));

    StorageContainerException ex = assertThrows(StorageContainerException.class, () ->
        dnClient.getContainerChecksumInfo(containerID, targetDN.getDatanodeDetails()));
    assertEquals(ContainerProtos.Result.IO_EXCEPTION, ex.getResult());
  }

  /**
   * Tests reading the container checksum info file from a datanode where the datanode fails to read the file from
   * the disk.
   */
  @Test
  public void testGetChecksumInfoServerIOError() throws Exception {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);
    // Pick a datanode and remove its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = getContainerChecksumFile(container.getContainerData());
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
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);

    // Pick a datanode and corrupt its checksum file.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = getContainerChecksumFile(container.getContainerData());
    Files.write(treeFile.toPath(), new byte[]{1, 2, 3},
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

    // Reading the file from the replica should fail when the client tries to deserialize it.
    assertThrows(InvalidProtocolBufferException.class, () -> dnClient.getContainerChecksumInfo(containerID,
        targetDN.getDatanodeDetails()));
  }

  @Test
  public void testGetEmptyChecksumInfo() throws Exception {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);

    // Pick a datanode and truncate its checksum file to zero length.
    HddsDatanodeService targetDN = cluster.getHddsDatanodes().get(0);
    Container<?> container = targetDN.getDatanodeStateMachine().getContainer()
        .getContainerSet().getContainer(containerID);
    File treeFile = getContainerChecksumFile(container.getContainerData());
    assertTrue(treeFile.exists());
    Files.write(treeFile.toPath(), new byte[]{},
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
    assertEquals(0, treeFile.length());

    // The client will get an empty byte string back. It should raise this as an error instead of returning a default
    // protobuf object.
    StorageContainerException ex = assertThrows(StorageContainerException.class, () ->
        dnClient.getContainerChecksumInfo(containerID, targetDN.getDatanodeDetails()));
    assertEquals(ContainerProtos.Result.IO_EXCEPTION, ex.getResult());
  }

  @Test
  public void testGetChecksumInfoSuccess() throws Exception {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    long containerID = writeDataAndGetContainer(true, volume, bucket);
    // Overwrite the existing tree with a custom one for testing. We will check that it is returned properly from the
    // API.
    ContainerMerkleTreeWriter tree = buildTestTree(conf);
    writeChecksumFileToDatanodes(containerID, tree);

    // Verify trees match on all replicas.
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

  @Test
  @Flaky("HDDS-13401")
  public void testContainerChecksumWithBlockMissing() throws Exception {
    // 1. Write data to a container.
    // Read the key back and check its hash.
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    Pair<Long, byte[]> containerAndData = getDataAndContainer(true, 20 * 1024 * 1024, volume, bucket);
    long containerID = containerAndData.getLeft();
    byte[] data = containerAndData.getRight();
    // Get the datanodes where the container replicas are stored.
    List<DatanodeDetails> dataNodeDetails = cluster.getStorageContainerManager().getContainerManager()
        .getContainerReplicas(ContainerID.valueOf(containerID))
        .stream().map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    assertEquals(3, dataNodeDetails.size());
    HddsDatanodeService hddsDatanodeService = cluster.getHddsDatanode(dataNodeDetails.get(0));
    DatanodeStateMachine datanodeStateMachine = hddsDatanodeService.getDatanodeStateMachine();
    Container<?> container = datanodeStateMachine.getContainer().getContainerSet().getContainer(containerID);
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    ContainerProtos.ContainerChecksumInfo oldContainerChecksumInfo = readChecksumFile(container.getContainerData());
    KeyValueHandler kvHandler = (KeyValueHandler) datanodeStateMachine.getContainer().getDispatcher()
        .getHandler(ContainerProtos.ContainerType.KeyValueContainer);

    BlockManager blockManager = kvHandler.getBlockManager();
    List<BlockData> blockDataList = blockManager.listBlock(container, -1, 100);
    String chunksPath = container.getContainerData().getChunksPath();
    long oldDataChecksum = oldContainerChecksumInfo.getContainerMerkleTree().getDataChecksum();

    // 2. Delete some blocks to simulate missing blocks.
    try (DBHandle db = BlockUtils.getDB(containerData, conf);
         BatchOperation op = db.getStore().getBatchHandler().initBatchOperation()) {
      for (int i = 0; i < blockDataList.size(); i += 2) {
        BlockData blockData = blockDataList.get(i);
        // Delete the block metadata from the container db
        db.getStore().getBlockDataTable().deleteWithBatch(op, containerData.getBlockKey(blockData.getLocalID()));
        // Delete the block file.
        Files.deleteIfExists(Paths.get(chunksPath + "/" + blockData.getBlockID().getLocalID() + ".block"));
      }
      db.getStore().getBatchHandler().commitBatchOperation(op);
      db.getStore().flushDB();
    }

    datanodeStateMachine.getContainer().getContainerSet().scanContainerWithoutGap(containerID, TEST_SCAN);
    waitForDataChecksumsAtSCM(containerID, 2);
    ContainerProtos.ContainerChecksumInfo containerChecksumAfterBlockDelete =
        readChecksumFile(container.getContainerData());
    long dataChecksumAfterBlockDelete = containerChecksumAfterBlockDelete.getContainerMerkleTree().getDataChecksum();
    // Checksum should have changed after block delete.
    assertNotEquals(oldDataChecksum, dataChecksumAfterBlockDelete);

    // 3. Reconcile the container.
    cluster.getStorageContainerLocationClient().reconcileContainer(containerID);
    // Compare and check if dataChecksum is same on all replicas.
    waitForDataChecksumsAtSCM(containerID, 1);
    ContainerProtos.ContainerChecksumInfo newContainerChecksumInfo = readChecksumFile(container.getContainerData());
    assertTreesSortedAndMatch(oldContainerChecksumInfo.getContainerMerkleTree(),
        newContainerChecksumInfo.getContainerMerkleTree());
    TestHelper.validateData(KEY_NAME, data, store, volume, bucket);
  }

  @Test
  public void testContainerChecksumChunkCorruption() throws Exception {
    // 1. Write data to a container.
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    Pair<Long, byte[]> containerAndData = getDataAndContainer(true, 20 * 1024 * 1024, volume, bucket);
    long containerID = containerAndData.getLeft();
    byte[] data = containerAndData.getRight();
    // Get the datanodes where the container replicas are stored.
    List<DatanodeDetails> dataNodeDetails = cluster.getStorageContainerManager().getContainerManager()
        .getContainerReplicas(ContainerID.valueOf(containerID))
        .stream().map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    assertEquals(3, dataNodeDetails.size());
    HddsDatanodeService hddsDatanodeService = cluster.getHddsDatanode(dataNodeDetails.get(0));
    DatanodeStateMachine datanodeStateMachine = hddsDatanodeService.getDatanodeStateMachine();
    Container<?> container = datanodeStateMachine.getContainer().getContainerSet().getContainer(containerID);
    ContainerProtos.ContainerChecksumInfo oldContainerChecksumInfo = readChecksumFile(container.getContainerData());
    KeyValueHandler kvHandler = (KeyValueHandler) datanodeStateMachine.getContainer().getDispatcher()
        .getHandler(ContainerProtos.ContainerType.KeyValueContainer);

    BlockManager blockManager = kvHandler.getBlockManager();
    List<BlockData> blockDatas = blockManager.listBlock(container, -1, 100);
    long oldDataChecksum = oldContainerChecksumInfo.getContainerMerkleTree().getDataChecksum();

    // 2. Corrupt every block in one replica.
    for (BlockData blockData : blockDatas) {
      long blockID = blockData.getLocalID();
      TestContainerCorruptions.CORRUPT_BLOCK.applyTo(container, blockID);
    }

    datanodeStateMachine.getContainer().getContainerSet().scanContainerWithoutGap(containerID, TEST_SCAN);
    waitForDataChecksumsAtSCM(containerID, 2);
    ContainerProtos.ContainerChecksumInfo containerChecksumAfterChunkCorruption =
        readChecksumFile(container.getContainerData());
    long dataChecksumAfterAfterChunkCorruption = containerChecksumAfterChunkCorruption
        .getContainerMerkleTree().getDataChecksum();
    // Checksum should have changed after chunk corruption.
    assertNotEquals(oldDataChecksum, dataChecksumAfterAfterChunkCorruption);

    // 4. Reconcile the container.
    cluster.getStorageContainerLocationClient().reconcileContainer(containerID);
    // Compare and check if dataChecksum is same on all replicas.
    waitForDataChecksumsAtSCM(containerID, 1);
    ContainerProtos.ContainerChecksumInfo newContainerChecksumInfo = readChecksumFile(container.getContainerData());
    assertTreesSortedAndMatch(oldContainerChecksumInfo.getContainerMerkleTree(),
        newContainerChecksumInfo.getContainerMerkleTree());
    assertEquals(oldDataChecksum, newContainerChecksumInfo.getContainerMerkleTree().getDataChecksum());
    TestHelper.validateData(KEY_NAME, data, store, volume, bucket);
  }

  @Test
  @Flaky("HDDS-13401")
  public void testDataChecksumReportedAtSCM() throws Exception {
    // 1. Write data to a container.
    // Read the key back and check its hash.
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    Pair<Long, byte[]> containerAndData = getDataAndContainer(true, 20 * 1024 * 1024, volume, bucket);
    long containerID = containerAndData.getLeft();
    byte[] data = containerAndData.getRight();
    // Get the datanodes where the container replicas are stored.
    List<DatanodeDetails> dataNodeDetails = cluster.getStorageContainerManager().getContainerManager()
        .getContainerReplicas(ContainerID.valueOf(containerID))
        .stream().map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    assertEquals(3, dataNodeDetails.size());
    HddsDatanodeService hddsDatanodeService = cluster.getHddsDatanode(dataNodeDetails.get(0));
    DatanodeStateMachine datanodeStateMachine = hddsDatanodeService.getDatanodeStateMachine();
    Container<?> container = datanodeStateMachine.getContainer().getContainerSet().getContainer(containerID);
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    ContainerProtos.ContainerChecksumInfo oldContainerChecksumInfo = readChecksumFile(container.getContainerData());
    KeyValueHandler kvHandler = (KeyValueHandler) datanodeStateMachine.getContainer().getDispatcher()
        .getHandler(ContainerProtos.ContainerType.KeyValueContainer);

    long oldDataChecksum = oldContainerChecksumInfo.getContainerMerkleTree().getDataChecksum();
    // Check non-zero checksum after container close
    StorageContainerLocationProtocolClientSideTranslatorPB scmClient = cluster.getStorageContainerLocationClient();
    List<HddsProtos.SCMContainerReplicaProto> containerReplicas = scmClient.getContainerReplicas(containerID,
        ClientVersion.CURRENT.serialize());
    assertEquals(3, containerReplicas.size());
    for (HddsProtos.SCMContainerReplicaProto containerReplica: containerReplicas) {
      assertNotEquals(0, containerReplica.getDataChecksum());
    }

    // 2. Delete some blocks to simulate missing blocks.
    BlockManager blockManager = kvHandler.getBlockManager();
    List<BlockData> blockDataList = blockManager.listBlock(container, -1, 100);
    String chunksPath = container.getContainerData().getChunksPath();
    try (DBHandle db = BlockUtils.getDB(containerData, conf);
         BatchOperation op = db.getStore().getBatchHandler().initBatchOperation()) {
      for (int i = 0; i < blockDataList.size(); i += 2) {
        BlockData blockData = blockDataList.get(i);
        // Delete the block metadata from the container db
        db.getStore().getBlockDataTable().deleteWithBatch(op, containerData.getBlockKey(blockData.getLocalID()));
        // Delete the block file.
        Files.deleteIfExists(Paths.get(chunksPath + "/" + blockData.getBlockID().getLocalID() + ".block"));
      }
      db.getStore().getBatchHandler().commitBatchOperation(op);
      db.getStore().flushDB();
    }

    datanodeStateMachine.getContainer().getContainerSet().scanContainerWithoutGap(containerID, TEST_SCAN);
    waitForDataChecksumsAtSCM(containerID, 2);
    ContainerProtos.ContainerChecksumInfo containerChecksumAfterBlockDelete =
        readChecksumFile(container.getContainerData());
    long dataChecksumAfterBlockDelete = containerChecksumAfterBlockDelete.getContainerMerkleTree().getDataChecksum();
    // Checksum should have changed after block delete.
    assertNotEquals(oldDataChecksum, dataChecksumAfterBlockDelete);

    scmClient.reconcileContainer(containerID);
    waitForDataChecksumsAtSCM(containerID, 1);
    // Check non-zero checksum after container reconciliation
    containerReplicas = scmClient.getContainerReplicas(containerID, ClientVersion.CURRENT.serialize());
    assertEquals(3, containerReplicas.size());
    for (HddsProtos.SCMContainerReplicaProto containerReplica: containerReplicas) {
      assertNotEquals(0, containerReplica.getDataChecksum());
    }

    // Check non-zero checksum after datanode restart
    // Restarting all the nodes take more time in mini ozone cluster, so restarting only one node
    cluster.restartHddsDatanode(0, true);
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      cluster.restartStorageContainerManager(scm, false);
    }
    cluster.waitForClusterToBeReady();
    waitForDataChecksumsAtSCM(containerID, 1);
    containerReplicas = scmClient.getContainerReplicas(containerID, ClientVersion.CURRENT.serialize());
    assertEquals(3, containerReplicas.size());
    for (HddsProtos.SCMContainerReplicaProto containerReplica: containerReplicas) {
      assertNotEquals(0, containerReplica.getDataChecksum());
    }
    TestHelper.validateData(KEY_NAME, data, store, volume, bucket);
  }

  private void waitForDataChecksumsAtSCM(long containerID, int expectedSize) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        Set<Long> dataChecksums = cluster.getStorageContainerLocationClient().getContainerReplicas(containerID,
                ClientVersion.CURRENT.serialize()).stream()
            .map(HddsProtos.SCMContainerReplicaProto::getDataChecksum)
            .collect(Collectors.toSet());
        LOG.info("Waiting for {} total unique checksums from container {} to be reported to SCM. Currently {} unique" +
            "checksums are reported.", expectedSize, containerID, dataChecksums.size());
        return dataChecksums.size() == expectedSize;
      } catch (Exception ex) {
        return false;
      }
    }, 1000, 20000);
  }

  private Pair<Long, byte[]> getDataAndContainer(boolean close, int dataLen, String volumeName, String bucketName)
          throws Exception {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    byte[] data = randomAlphabetic(dataLen).getBytes(UTF_8);
    // Write Key
    try (OzoneOutputStream os = TestHelper.createKey(KEY_NAME, RATIS, THREE, dataLen, store, volumeName, bucketName)) {
      IOUtils.write(data, os);
    }

    long containerID = bucket.getKey(KEY_NAME).getOzoneKeyLocations().stream()
        .findFirst().get().getContainerID();
    if (close) {
      TestHelper.waitForContainerClose(cluster, containerID);
      TestHelper.waitForScmContainerState(cluster, containerID, HddsProtos.LifeCycleState.CLOSED);
    }
    return Pair.of(containerID, data);
  }

  private long writeDataAndGetContainer(boolean close, String volume, String bucket) throws Exception {
    return getDataAndContainer(close, 5, volume, bucket).getLeft();
  }

  public static void writeChecksumFileToDatanodes(long containerID, ContainerMerkleTreeWriter tree) throws Exception {
    // Write Container Merkle Tree
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      KeyValueHandler keyValueHandler =
          (KeyValueHandler) dn.getDatanodeStateMachine().getContainer().getDispatcher()
              .getHandler(ContainerProtos.ContainerType.KeyValueContainer);
      KeyValueContainer keyValueContainer =
          (KeyValueContainer) dn.getDatanodeStateMachine().getContainer().getController()
              .getContainer(containerID);
      if (keyValueContainer != null) {
        keyValueHandler.getChecksumManager().updateTree(keyValueContainer.getContainerData(), tree);
      }
    }
  }

  private static void setSecretKeysConfig() {
    // Secret key lifecycle configs.
    conf.set(HDDS_SECRET_KEY_ROTATE_CHECK_DURATION, "1s");
    conf.set(HDDS_SECRET_KEY_ROTATE_DURATION, "100s");
    conf.set(HDDS_SECRET_KEY_EXPIRY_DURATION, "500s");
    conf.set(DELEGATION_TOKEN_MAX_LIFETIME_KEY, "300s");
    conf.set(DELEGATION_REMOVER_SCAN_INTERVAL_KEY, "1s");

    // enable tokens
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);
  }

  private static void createCredentialsInKDC() throws Exception {
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, scmConfig.getKerberosPrincipal());
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    createPrincipal(testUserKeytab, testUserPrincipal);
  }

  private static void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private static void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private static void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = InetAddress.getLocalHost().getCanonicalHostName()
                      .toLowerCase();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());
    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);
    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY, spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE, spnegoKeytab.getAbsolutePath());
    conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY, ozoneKeytab.getAbsolutePath());
  }

  private static void startCluster() throws Exception {
    OzoneManager.setTestSecureOmFlag(true);
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId("SecureSCM")
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(1)
        .build();
    cluster.waitForClusterToBeReady();
    rpcClient = OzoneClientFactory.getRpcClient(conf);
    store = rpcClient.getObjectStore();
    SecretKeyClient secretKeyClient =  cluster.getStorageContainerManager().getSecretKeyManager();
    CertificateClient certClient = cluster.getStorageContainerManager().getScmCertificateClient();
    dnClient = new DNContainerOperationClient(conf, certClient, secretKeyClient);
  }
}
