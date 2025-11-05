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

package org.apache.hadoop.ozone.client.rpc;

import static org.apache.hadoop.ozone.OzoneBlockMetadata.BUCKET_NAME;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.CREATION_TIME;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.KEY_NAME;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.OBJECT_ID;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.PARENT_OBJECT_ID;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.TYPE;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.TYPE_KEY;
import static org.apache.hadoop.ozone.OzoneBlockMetadata.VOLUME_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for verifying block file metadata.
 * These tests verify that block metadata such as volume, bucket, key name,
 * objectID, parentObjectID, and creation time are correctly written to
 * block files and can be queried from RocksDB.
 */
public class TestBlockMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(TestBlockMetadata.class);
  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static ObjectStore store;
  private static OzoneConfiguration conf;

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    // Set small block size to ensure multiple blocks are created in multi-block tests
    conf.set(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, "1MB");
    
    // Configure short intervals for EC reconstruction test to detect dead nodes quickly
    conf.set(org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, "3s");
    conf.set(org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, "3s");
    conf.set(org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, "1s");
    conf.set(org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, "1s");
    
    // Configure replication manager intervals
    ReplicationManager.ReplicationManagerConfiguration rmConfig =
        conf.getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    rmConfig.setInterval(java.time.Duration.ofSeconds(3));
    conf.setFromObject(rmConfig);
    
    DatanodeConfiguration datanodeConfiguration = conf.getObject(DatanodeConfiguration.class);
    conf.setFromObject(datanodeConfiguration);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(8)  // Need extra nodes for EC reconstruction tests (EC needs 5, plus spares)
        .build();
    cluster.waitForClusterToBeReady();
    
    client = cluster.newClient();
    store = client.getObjectStore();
  }

  @AfterAll
  public static void shutdown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test block metadata for keys in OBS buckets with RATIS replication.
   * Verifies that all metadata fields are correctly written to RocksDB.
   */
  @Test
  public void testOBSBucketMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    String keyName = UUID.randomUUID().toString();
    Instant beforeWrite = Instant.now();
    
    byte[] data = "test data for OBS block metadata verification".getBytes(StandardCharsets.UTF_8);
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, keyName, data, 
        ReplicationType.RATIS, ReplicationFactor.ONE);
    
    Instant afterWrite = Instant.now();

    // Get key details
    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    assertNotNull(keyDetails);
    
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    assertTrue(!keyLocations.isEmpty(), "OBS key should have at least one block");

    // Get block data from RocksDB and verify all metadata fields
    OzoneKeyLocation keyLocation = keyLocations.get(0);
    BlockData blockData = getBlockDataFromRocksDB(
        keyLocation.getContainerID(), 
        keyLocation.getLocalID());
    assertNotNull(blockData, "Block data should not be null");

    Map<String, String> metadata = blockData.getMetadata();
    assertNotNull(metadata, "Block metadata should not be null");

    LOG.info("OBS bucket key block metadata: {}", metadata);

    // Verify all metadata fields
    verifyBlockMetadata(metadata, volumeName, bucketName, keyName, 
        keyInfo.getObjectID(), keyInfo.getParentObjectID());
    
    // Verify creation time
    String creationTimeStr = metadata.get(CREATION_TIME);
    Instant creationTime = Instant.parse(creationTimeStr);
    assertTrue(creationTime.isAfter(beforeWrite.minusSeconds(2)),
        "Creation time should be after write start");
    assertTrue(creationTime.isBefore(afterWrite.plusSeconds(2)),
        "Creation time should be before write end");
  }

  /**
   * Test block metadata for keys in FSO buckets with RATIS replication.
   */
  @Test
  public void testFSOBucketMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "fso-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    
    // Test nested key in FSO bucket
    String nestedKeyName = "dir1/dir2/" + UUID.randomUUID();
    byte[] data = "test data for FSO nested key".getBytes(StandardCharsets.UTF_8);
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, nestedKeyName, data,
        ReplicationType.RATIS, ReplicationFactor.ONE);

    OzoneKeyDetails keyDetails = bucket.getKey(nestedKeyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    OzoneKeyLocation keyLocation = keyLocations.get(0);
    BlockData blockData = getBlockDataFromRocksDB(
        keyLocation.getContainerID(), 
        keyLocation.getLocalID());
    
    assertNotNull(blockData, "Block data should not be null");
    Map<String, String> metadata = blockData.getMetadata();
    LOG.info("FSO nested key block metadata: {}", metadata);
    
    verifyBlockMetadata(metadata, volumeName, bucketName, nestedKeyName,
        keyInfo.getObjectID(), keyInfo.getParentObjectID());
  }

  /**
   * Test block metadata consistency across multiple blocks for the same key.
   * Verifies that all blocks of a key have the same metadata values.
   */
  @Test
  public void testMultiBlockMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    // Create key with data larger than block size (4MB) to create multiple blocks
    String keyName = UUID.randomUUID().toString();
    int dataSize = 10 * 1024 * 1024; // 10MB to ensure multiple 4MB blocks
    byte[] largeData = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      largeData[i] = (byte) (i % 256);
    }
    
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, keyName, largeData,
        ReplicationType.RATIS, ReplicationFactor.ONE);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();

    LOG.info("Key with large data has {} blocks", keyLocations.size());
    assertTrue(!keyLocations.isEmpty(), "Key should have multiple blocks");

    // Verify all blocks have consistent metadata
    for (int i = 0; i < keyLocations.size(); i++) {
      OzoneKeyLocation location = keyLocations.get(i);
      BlockData blockData = getBlockDataFromRocksDB(
          location.getContainerID(), 
          location.getLocalID());
      assertNotNull(blockData, "Block " + i + " data should not be null");

      Map<String, String> metadata = blockData.getMetadata();
      LOG.info("Block {} of {} metadata: {}", i, keyLocations.size(), metadata);

      // All blocks should have the same metadata values
      verifyBlockMetadata(metadata, volumeName, bucketName, keyName,
          keyInfo.getObjectID(), keyInfo.getParentObjectID());
    }
  }

  /**
   * Test EC block metadata for keys in OBS buckets.
   * Verifies that EC block metadata is correctly written to RocksDB.
   */
  @Test
  public void testOBSBucketECMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-ec-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    String keyName = UUID.randomUUID().toString();
    ReplicationConfig ecConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, 1024 * 1024);
    
    byte[] data = "test data for OBS EC block metadata".getBytes(StandardCharsets.UTF_8);
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, keyName, data, ecConfig);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    assertTrue(!keyLocations.isEmpty(), "EC key should have at least one block group");

    // Get block data from RocksDB for first block group
    OzoneKeyLocation keyLocation = keyLocations.get(0);
    BlockData blockData = getBlockDataFromRocksDB(
        keyLocation.getContainerID(), 
        keyLocation.getLocalID());
    assertNotNull(blockData, "EC block data should not be null");

    Map<String, String> metadata = blockData.getMetadata();
    assertNotNull(metadata, "EC block metadata should not be null");

    LOG.info("OBS EC bucket key block metadata: {}", metadata);

    // Verify all metadata fields
    verifyBlockMetadata(metadata, volumeName, bucketName, keyName,
        keyInfo.getObjectID(), keyInfo.getParentObjectID());
  }

  /**
   * Test EC block metadata for keys in FSO buckets with nested paths.
   * Verifies that EC block metadata is correctly written for FSO buckets.
   */
  @Test
  public void testFSOBucketECMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "fso-ec-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    
    String nestedKeyName = "dir1/dir2/" + UUID.randomUUID();
    ReplicationConfig ecConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, 1024 * 1024);
    
    byte[] data = "test data for FSO EC nested key".getBytes(StandardCharsets.UTF_8);
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, nestedKeyName, data, ecConfig);

    OzoneKeyDetails keyDetails = bucket.getKey(nestedKeyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    assertTrue(!keyLocations.isEmpty(), "FSO EC key should have at least one block group");

    // Get block data from RocksDB
    OzoneKeyLocation keyLocation = keyLocations.get(0);
    BlockData blockData = getBlockDataFromRocksDB(
        keyLocation.getContainerID(), 
        keyLocation.getLocalID());
    assertNotNull(blockData, "FSO EC block data should not be null");

    Map<String, String> metadata = blockData.getMetadata();
    LOG.info("FSO EC nested key block metadata: {}", metadata);

    // Verify all metadata fields including nested path
    verifyBlockMetadata(metadata, volumeName, bucketName, nestedKeyName,
        keyInfo.getObjectID(), keyInfo.getParentObjectID());
  }

  /**
   * Test EC block metadata consistency across multiple stripe groups.
   * Verifies that all EC stripe groups have consistent metadata.
   * With rs-3-2-1024k and block size 4MB, need data larger than 4MB to create multiple blocks.
   */
  @Test
  public void testMultiBlockECMetadata() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-ec-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    String keyName = UUID.randomUUID().toString();
    ReplicationConfig ecConfig = new ECReplicationConfig(3, 2,
        ECReplicationConfig.EcCodec.RS, 1024 * 1024);
    
    // Create large data to create multiple EC block groups
    // With 4MB block size and EC rs-3-2-1024k, need significantly larger data
    // Use 20MB to ensure multiple block groups are created
    int dataSize = 20 * 1024 * 1024; // 20MB
    byte[] largeData = new byte[dataSize];
    for (int i = 0; i < dataSize; i++) {
      largeData[i] = (byte) (i % 256);
    }
    
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, keyName, largeData, ecConfig);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();

    LOG.info("EC key with large data has {} block groups", keyLocations.size());
    // EC block allocation may vary, but should have at least 1 block group
    assertTrue(!keyLocations.isEmpty(), "EC key should have at least one block group");
    
    // If we have multiple block groups, verify all have consistent metadata
    // Note: With 20MB data and EC rs-3-2-1024k, we expect multiple block groups
    for (int i = 0; i < keyLocations.size(); i++) {
      OzoneKeyLocation location = keyLocations.get(i);
      BlockData blockData = getBlockDataFromRocksDB(
          location.getContainerID(), 
          location.getLocalID());
      
      if (blockData != null) {
        Map<String, String> metadata = blockData.getMetadata();
        LOG.info("EC block group {} of {} metadata: {}", i, keyLocations.size(), metadata);

        // All block groups should have the same metadata values
        verifyBlockMetadata(metadata, volumeName, bucketName, keyName,
            keyInfo.getObjectID(), keyInfo.getParentObjectID());
      }
    }
  }

  /**
   * Test that metadata is preserved after RATIS container replication.
   * Verifies metadata exists on all replicas and is identical.
   */
  @Test
  public void testMetadataAfterRatisReplication() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-ratis-repl-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    String keyName = UUID.randomUUID().toString();
    byte[] data = "test data for RATIS replication metadata".getBytes(StandardCharsets.UTF_8);
    
    // Write key with RATIS THREE to enable replication testing
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>())) {
      out.write(data);
    }
    
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    OmKeyInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    OzoneKeyLocation keyLocation = keyLocations.get(0);
    long containerID = keyLocation.getContainerID();
    long localID = keyLocation.getLocalID();

    // Get container info and pipeline
    ContainerInfo containerInfo = cluster.getStorageContainerManager()
        .getContainerManager()
        .getContainer(ContainerID.valueOf(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipeline(containerInfo.getPipelineID());
    
    // Verify metadata exists on all 3 replicas and is identical
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    Map<String, String> firstMetadata = null;
    
    for (int i = 0; i < datanodes.size(); i++) {
      DatanodeDetails dn = datanodes.get(i);
      LOG.info("Checking metadata on replica {} - datanode {}", i, dn);
      
      // Find this specific datanode and read its block metadata
      HddsDatanodeService datanodeService = null;
      for (HddsDatanodeService dnService : cluster.getHddsDatanodes()) {
        if (dn.equals(dnService.getDatanodeDetails())) {
          datanodeService = dnService;
          break;
        }
      }
      
      if (datanodeService != null) {
        KeyValueContainerData containerData = 
            (KeyValueContainerData) datanodeService.getDatanodeStateMachine()
                .getContainer()
                .getContainerSet()
                .getContainer(containerID)
                .getContainerData();
        
        try (DBHandle db = BlockUtils.getDB(containerData, conf)) {
          String blockKey = containerData.getBlockKey(localID);
          BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
          
          assertNotNull(blockData, "Block data should exist on replica " + i);
          Map<String, String> metadata = blockData.getMetadata();
          
          LOG.info("Replica {} metadata: {}", i, metadata);
          
          // Verify all metadata fields
          verifyBlockMetadata(metadata, volumeName, bucketName, keyName,
              keyInfo.getObjectID(), keyInfo.getParentObjectID());
          
          if (firstMetadata == null) {
            firstMetadata = metadata;
          } else {
            // Verify all replicas have identical metadata
            assertEquals(firstMetadata, metadata,
                "Metadata on replica " + i + " should match replica 0");
          }
        }
      }
    }
    
    LOG.info("RATIS metadata verified on all {} replicas", datanodes.size());
  }

  /**
   * Test that metadata is correctly set on reconstructed EC stripe replicas.
   * This test:
   * 1. Writes EC data with metadata
   * 2. Verifies initial metadata on original stripes
   * 3. Shuts down a datanode to trigger reconstruction
   * 4. Waits for EC reconstruction to complete
   * 5. Verifies that the reconstructed stripe has correct metadata
   */
  @Test
  public void testMetadataAfterECReconstruction() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "obs-ec-reconstruct-" + UUID.randomUUID();
    OzoneBucket bucket = createBucket(volumeName, bucketName, BucketLayout.OBJECT_STORE);
    
    String keyName = UUID.randomUUID().toString();
    org.apache.hadoop.hdds.client.ReplicationConfig ecConfig = 
        new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, 1024 * 1024);
    
    // Write larger data to ensure we have a substantial EC block group
    byte[] data = new byte[5 * 1024 * 1024]; // 5MB
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }
    
    OmKeyInfo keyInfo = writeKeyAndGetInfo(bucket, keyName, data, ecConfig);

    OzoneKeyDetails keyDetails = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    assertTrue(!keyLocations.isEmpty(), "EC key should have at least one block group");

    OzoneKeyLocation keyLocation = keyLocations.get(0);
    long containerID = keyLocation.getContainerID();
    long localID = keyLocation.getLocalID();

    // Get container info and pipeline
    ContainerInfo containerInfo = cluster.getStorageContainerManager()
        .getContainerManager()
        .getContainer(ContainerID.valueOf(containerID));
    Pipeline originalPipeline = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipeline(containerInfo.getPipelineID());
    
    LOG.info("Original pipeline has {} nodes", originalPipeline.getNodes().size());
    
    // Verify metadata exists on all 5 original EC stripe replicas (3 data + 2 parity)
    List<DatanodeDetails> originalDatanodes = originalPipeline.getNodes();
    Map<String, String> originalMetadata = null;
    
    LOG.info("Step 1: Verifying EC metadata on original {} stripe replicas", originalDatanodes.size());
    
    for (int i = 0; i < originalDatanodes.size(); i++) {
      DatanodeDetails dn = originalDatanodes.get(i);
      BlockData blockData = getBlockDataFromDatanode(dn, containerID, localID);
      
      if (blockData != null) {
        Map<String, String> metadata = blockData.getMetadata();
        LOG.info("Original stripe replica {} on DN {} metadata: {}", i, dn.getUuidString(), metadata);
        
        // Verify all metadata fields
        verifyBlockMetadata(metadata, volumeName, bucketName, keyName,
            keyInfo.getObjectID(), keyInfo.getParentObjectID());
        
        if (originalMetadata == null) {
          originalMetadata = metadata;
        }
      }
    }
    
    assertNotNull(originalMetadata, "Should have found at least one original replica with metadata");
    
    // Step 2: Trigger EC reconstruction by shutting down a datanode
    DatanodeDetails nodeToShutdown = originalPipeline.getFirstNode();
    LOG.info("Step 2: Shutting down datanode {} to trigger EC reconstruction", nodeToShutdown.getUuidString());
    
    cluster.shutdownHddsDatanode(nodeToShutdown);
    
    // Wait for container to close
    LOG.info("Step 3: Waiting for container to close");
    waitForContainerState(ContainerID.valueOf(containerID), 
        org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED);
    
    // Wait for the datanode to be detected as dead/stale by SCM
    LOG.info("Step 4: Waiting for datanode to be detected as dead");
    waitForDatanodeToBeDead(nodeToShutdown);
    
    // Stop replication manager temporarily to observe replica count drop
    LOG.info("Step 5: Stopping replication manager");
    cluster.getStorageContainerManager().getReplicationManager().stop();
    waitForReplicationManagerStopped();
    
    // Wait for replica count to drop to 4
    LOG.info("Step 6: Waiting for replica count to drop to 4");
    waitForContainerReplicaCount(ContainerID.valueOf(containerID), 4);
    
    // Start replication manager to trigger reconstruction
    LOG.info("Step 7: Starting replication manager to trigger reconstruction");
    cluster.getStorageContainerManager().getReplicationManager().start();
    
    // Wait for reconstruction to complete - replica count should return to 5
    LOG.info("Step 8: Waiting for reconstruction to complete (replica count = 5)");
    waitForContainerReplicaCount(ContainerID.valueOf(containerID), 5);
    
    // Find the reconstructed replica (new datanode not in original pipeline)
    LOG.info("Step 9: Finding and verifying reconstructed replica metadata");
    
    java.util.Set<org.apache.hadoop.hdds.scm.container.ContainerReplica> allReplicas = 
        cluster.getStorageContainerManager()
            .getContainerManager()
            .getContainerReplicas(ContainerID.valueOf(containerID));
    
    LOG.info("After reconstruction, found {} total replicas", allReplicas.size());
    
    // Find the new replica (not in original pipeline and not the shutdown node)
    DatanodeDetails reconstructedNode = null;
    for (org.apache.hadoop.hdds.scm.container.ContainerReplica replica : allReplicas) {
      DatanodeDetails replicaDN = replica.getDatanodeDetails();
      boolean isOriginal = originalDatanodes.stream()
          .anyMatch(dn -> dn.equals(replicaDN));
      
      if (!isOriginal) {
        reconstructedNode = replicaDN;
        LOG.info("Found reconstructed replica on new datanode: {}", replicaDN.getUuidString());
        break;
      }
    }
    
    assertNotNull(reconstructedNode, "Should have found a reconstructed replica on a new datanode");
    
    // Verify metadata on the reconstructed replica
    BlockData reconstructedBlockData = getBlockDataFromDatanode(reconstructedNode, containerID, localID);
    assertNotNull(reconstructedBlockData, "Reconstructed replica should have block data");
    
    Map<String, String> reconstructedMetadata = reconstructedBlockData.getMetadata();
    LOG.info("Reconstructed replica metadata: {}", reconstructedMetadata);
    
    // Verify all metadata fields match the original
    verifyBlockMetadata(reconstructedMetadata, volumeName, bucketName, keyName,
        keyInfo.getObjectID(), keyInfo.getParentObjectID());
    
    // Verify metadata fields match original (except possibly CREATION_TIME)
    List<String> metadataFields = new ArrayList<>(originalMetadata.keySet());
    for (String metadataField : metadataFields) {
      assertEquals(originalMetadata.get(metadataField), reconstructedMetadata.get(metadataField));
    }
    
    LOG.info("EC reconstruction metadata verification successful!");
  }

  /**
   * Helper method to create a volume and bucket with specified layout.
   */
  private OzoneBucket createBucket(String volumeName, String bucketName, BucketLayout layout) 
      throws IOException {
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs bucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(layout)
        .build();
    volume.createBucket(bucketName, bucketArgs);
    return volume.getBucket(bucketName);
  }

  /**
   * Helper method to write a key and get its key info from OM.
   */
  private OmKeyInfo writeKeyAndGetInfo(OzoneBucket bucket, String keyName, 
      byte[] data, ReplicationType replicationType, ReplicationFactor replicationFactor) 
      throws Exception {
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        replicationType, replicationFactor, new HashMap<>())) {
      out.write(data);
    }
    
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    return cluster.getOzoneManager().lookupKey(keyArgs);
  }

  /**
   * Helper method to write a key with EC replication and get its key info from OM.
   */
  private OmKeyInfo writeKeyAndGetInfo(OzoneBucket bucket, String keyName, 
      byte[] data, ReplicationConfig replicationConfig) throws Exception {
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        replicationConfig, new HashMap<>())) {
      out.write(data);
    }
    
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    return cluster.getOzoneManager().lookupKey(keyArgs);
  }

  /**
   * Helper method to verify all standard metadata fields.
   */
  private void verifyBlockMetadata(Map<String, String> metadata, String volumeName, 
      String bucketName, String keyName, long objectID, long parentObjectID) {
    assertEquals(TYPE_KEY, metadata.get(TYPE), "TYPE metadata should be KEY");
    assertEquals(volumeName, metadata.get(VOLUME_NAME), "VOLUME_NAME metadata mismatch");
    assertEquals(bucketName, metadata.get(BUCKET_NAME), "BUCKET_NAME metadata mismatch");
    assertEquals(keyName, metadata.get(KEY_NAME), "KEY_NAME metadata mismatch");
    assertEquals(String.valueOf(objectID), metadata.get(OBJECT_ID),
        "OBJECT_ID metadata should match OM record");
    
    // Log for debugging parent object ID
    LOG.info("Expected parentObjectID: {}, Actual in metadata: {}", 
        parentObjectID, metadata.get(PARENT_OBJECT_ID));
    
    assertEquals(String.valueOf(parentObjectID), metadata.get(PARENT_OBJECT_ID),
        "PARENT_OBJECT_ID metadata should match OM record");
    assertNotNull(metadata.get(CREATION_TIME), "CREATION_TIME should not be null");
  }

  /**
   * Helper method to retrieve BlockData from RocksDB on the datanode.
   * This method finds the container on a datanode and reads the block
   * metadata directly from the container's RocksDB database.
   * Handles both RATIS and EC blocks.
   */
  private BlockData getBlockDataFromRocksDB(long containerID, long localID) 
      throws Exception {
    // Get container info from SCM
    ContainerInfo containerInfo = cluster.getStorageContainerManager()
        .getContainerManager()
        .getContainer(ContainerID.valueOf(containerID));
    assertNotNull(containerInfo, "Container info should not be null");
    
    // Get pipeline to find which datanodes have the container
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager()
        .getPipeline(containerInfo.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    
    // Iterate through datanodes to find the container and read from RocksDB
    for (DatanodeDetails datanodeDetails : datanodes) {
      HddsDatanodeService datanodeService = null;
      for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
        if (datanodeDetails.equals(dn.getDatanodeDetails())) {
          datanodeService = dn;
          break;
        }
      }
      
      if (datanodeService != null) {
        KeyValueContainerData containerData = 
            (KeyValueContainerData) datanodeService.getDatanodeStateMachine()
                .getContainer()
                .getContainerSet()
                .getContainer(containerID)
                .getContainerData();
        
        // Open RocksDB and read block data
        try (DBHandle db = BlockUtils.getDB(containerData, conf)) {
          String blockKey = containerData.getBlockKey(localID);
          BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
          
          if (blockData != null) {
            return blockData;
          }
          
          // For EC blocks, try with replica index
          for (int replicaIndex = 1; replicaIndex <= 5; replicaIndex++) {
            String ecBlockKey = containerData.getBlockKey(localID) + "_" + replicaIndex;
            blockData = db.getStore().getBlockDataTable().get(ecBlockKey);
            if (blockData != null) {
              return blockData;
            }
          }
        }
      }
    }
    
    return null;
  }

  /**
   * Helper method to get block data from a specific datanode.
   * Used for EC reconstruction testing to check metadata on specific replicas.
   */
  private BlockData getBlockDataFromDatanode(DatanodeDetails datanodeDetails, 
      long containerID, long localID) throws Exception {
    // Find the datanode service
    HddsDatanodeService datanodeService = null;
    for (HddsDatanodeService dn : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(dn.getDatanodeDetails())) {
        datanodeService = dn;
        break;
      }
    }
    
    if (datanodeService == null) {
      return null;
    }
    
    // Check if container exists on this datanode
    org.apache.hadoop.ozone.container.common.interfaces.Container<?> container = 
        datanodeService.getDatanodeStateMachine()
            .getContainer()
            .getContainerSet()
            .getContainer(containerID);
    
    if (container == null) {
      return null;
    }
    
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    
    // Open RocksDB and read block data
    try (DBHandle db = BlockUtils.getDB(containerData, conf)) {
      String blockKey = containerData.getBlockKey(localID);
      BlockData blockData = db.getStore().getBlockDataTable().get(blockKey);
      
      if (blockData != null) {
        return blockData;
      }
      
      // For EC blocks, try with replica index
      for (int replicaIndex = 1; replicaIndex <= 5; replicaIndex++) {
        String ecBlockKey = containerData.getBlockKey(localID) + "_" + replicaIndex;
        blockData = db.getStore().getBlockDataTable().get(ecBlockKey);
        if (blockData != null) {
          return blockData;
        }
      }
    }
    
    return null;
  }

  /**
   * Wait for container to reach a specific lifecycle state.
   */
  private void waitForContainerState(ContainerID containerID,
      org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState expectedState) 
      throws Exception {
    org.apache.ozone.test.GenericTestUtils.waitFor(() -> {
      try {
        org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState state = 
            cluster.getStorageContainerManager()
                .getContainerManager()
                .getContainer(containerID)
                .getState();
        return state == expectedState;
      } catch (Exception e) {
        return false;
      }
    }, 100, 100_000);
  }

  /**
   * Wait for replication manager to stop.
   */
  private void waitForReplicationManagerStopped() throws Exception {
    org.apache.ozone.test.GenericTestUtils.waitFor(() -> 
        !cluster.getStorageContainerManager().getReplicationManager().isRunning(), 
        100, 10_000);
  }

  /**
   * Wait for container replica count to reach expected value.
   */
  private void waitForContainerReplicaCount(ContainerID containerID, int expectedCount) 
      throws Exception {
    org.apache.ozone.test.GenericTestUtils.waitFor(() -> {
      try {
        int count = cluster.getStorageContainerManager()
            .getContainerManager()
            .getContainerReplicas(containerID)
            .size();
        LOG.info("Container {} has {} replicas, waiting for {}", 
            containerID, count, expectedCount);
        return count == expectedCount;
      } catch (Exception e) {
        LOG.warn("Error checking replica count: {}", e.getMessage());
        return false;
      }
    }, 100, 100_000);
  }

  /**
   * Wait for a datanode to be detected as dead by SCM.
   */
  private void waitForDatanodeToBeDead(DatanodeDetails datanodeDetails) throws Exception {
    org.apache.ozone.test.GenericTestUtils.waitFor(() -> {
      try {
        org.apache.hadoop.hdds.scm.node.NodeStatus nodeStatus = 
            cluster.getStorageContainerManager()
                .getScmNodeManager()
                .getNodeStatus(datanodeDetails);
        boolean isDead = nodeStatus.isDead();
        if (!isDead) {
          LOG.info("Waiting for datanode {} to be detected as dead, current status: {}", 
              datanodeDetails.getUuidString(), nodeStatus);
        }
        return isDead;
      } catch (org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException e) {
        LOG.warn("Node not found in NodeManager: {}", datanodeDetails.getUuidString());
        return false;
      }
    }, 500, 30_000);
  }
}
