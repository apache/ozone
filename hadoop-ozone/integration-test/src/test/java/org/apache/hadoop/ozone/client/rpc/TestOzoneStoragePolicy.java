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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.storagepolicy.CheckStoragePolicySubCommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.CreateFileOptions;
import org.apache.hadoop.ozone.client.protocol.CreateKeyOptions;
import org.apache.hadoop.ozone.client.protocol.MultipartUploadOptions;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Verifies that a key honours its storage policy end to end: the policy is
 * persisted on the key, SCM allocates the container on the policy's creation
 * tier, and the block physically lands on a datanode volume of that tier.
 *
 * Each test builds its own cluster because the datanode volume topology differs
 * per case.
 */
public class TestOzoneStoragePolicy {

  private static final ReplicationConfig RATIS_THREE =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private static final ECReplicationConfig EC_RS_3_2 = new ECReplicationConfig("RS-3-2-1024k");

  private static final List<BucketLayout> FILE_BUCKET_LAYOUTS =
      Arrays.asList(BucketLayout.FILE_SYSTEM_OPTIMIZED, BucketLayout.LEGACY);

  private MiniOzoneCluster cluster;
  private OzoneClient ozClient;
  private ObjectStore store;
  private OzoneManager ozoneManager;
  private ContainerManager containerManager;
  private PipelineManager pipelineManager;
  private OzoneConfiguration conf;

  /**
   * Starts a cluster whose datanodes each expose one volume per storage type, so
   * every storage policy has a matching volume to land on.
   */
  private void startCluster(List<List<StorageType>> storageTypeList)
      throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    conf.setStorageSize(OZONE_SCM_BLOCK_SIZE, 4, StorageUnit.MB);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(storageTypeList.size())
        .setNumDataVolumes(storageTypeList.get(0).size())
        .setDatanodeStorageType(storageTypeList)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitTobeOutOfSafeMode();

    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    ozoneManager = cluster.getOzoneManager();
    containerManager = cluster.getStorageContainerManager().getContainerManager();
    pipelineManager = cluster.getStorageContainerManager().getPipelineManager();
  }

  private static List<List<StorageType>> allTierTopology(int datanodes) {
    List<StorageType> volumes =
        Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE);
    return Collections.nCopies(datanodes, volumes);
  }

  @AfterEach
  public void shutdownCluster() {
    IOUtils.closeQuietly(ozClient);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * A key created with an explicit storage policy records that policy and lands
   * on the policy's creation tier, in SCM and on the datanodes.
   */
  @Test
  public void testKeyStoragePolicyIsHonoured() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    for (OzoneStoragePolicy storagePolicy : OzoneStoragePolicy.values()) {
      OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
      OmKeyInfo keyInfo = createKeyAndLookup(bucket, storagePolicy);

      assertEquals(keyInfo.getDataSize(), policyBytes(storagePolicy, storagePolicy.getCreationTier(), "preferred"));
      assertKeyInfo(keyInfo, storagePolicy);
      assertScmContainerAndPipeline(keyInfo, storagePolicy.getCreationTier());
      assertDatanodeContainerAndBlock(keyInfo, storagePolicy.getCreationTier());
    }
  }

  /**
   * A key created without a storage policy inherits the bucket's policy, and an
   * explicit key level policy overrides the bucket's.
   */
  @Test
  public void testKeyInheritsAndOverridesBucketStoragePolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneStoragePolicy bucketPolicy = OzoneStoragePolicy.COLD;
    OzoneStoragePolicy keyPolicy = OzoneStoragePolicy.HOT;

    OzoneBucket bucket = createBucket(bucketPolicy, BucketLayout.OBJECT_STORE);
    assertEquals(bucketPolicy, bucket.getStoragePolicy());

    // No key level policy: the bucket's policy applies.
    OmKeyInfo inherited = createKeyAndLookup(bucket, null);
    assertKeyInfo(inherited, bucketPolicy);
    assertScmContainerAndPipeline(inherited, bucketPolicy.getCreationTier());
    assertDatanodeContainerAndBlock(inherited, bucketPolicy.getCreationTier());

    // Explicit key level policy wins over the bucket's.
    OmKeyInfo overridden = createKeyAndLookup(bucket, keyPolicy);
    assertKeyInfo(overridden, keyPolicy);
    assertScmContainerAndPipeline(overridden, keyPolicy.getCreationTier());
    assertDatanodeContainerAndBlock(overridden, keyPolicy.getCreationTier());
  }

  /**
   * With neither a key nor a bucket policy, the key falls back to the cluster
   * default rather than being left without one.
   */
  @Test
  public void testKeyWithoutAnyPolicyUsesClusterDefault() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
    OmKeyInfo keyInfo = createKeyAndLookup(bucket, null);

    assertKeyInfo(keyInfo, OzoneStoragePolicy.getDefaultPolicy());
    assertScmContainerAndPipeline(keyInfo,
        OzoneStoragePolicy.getDefaultPolicy().getCreationTier());
  }

  /**
   * Creating a file with a storage policy works for every file bucket layout. All
   * layouts share one cluster, because the scenarios differ only in the bucket
   * they use.
   */
  @Test
  public void testCreateFileWithStoragePolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    for (BucketLayout bucketLayout : FILE_BUCKET_LAYOUTS) {
      assertFileStoragePolicyIsHonoured(bucketLayout);
      assertFileInheritsBucketPolicyOrClusterDefault(bucketLayout);
      assertMultiBlockFileAndOverwriteHonourPolicy(bucketLayout);
    }
  }

  /**
   * A file created with an explicit storage policy records that policy and lands
   * on the policy's creation tier, in SCM and on the datanodes.
   */
  private void assertFileStoragePolicyIsHonoured(BucketLayout bucketLayout)
      throws IOException {
    for (OzoneStoragePolicy storagePolicy : OzoneStoragePolicy.values()) {
      OzoneBucket bucket = createBucket(null, bucketLayout);
      OmKeyInfo keyInfo = createFileAndLookup(bucket, storagePolicy);

      assertKeyInfo(keyInfo, storagePolicy);
      assertScmContainerAndPipeline(keyInfo, storagePolicy.getCreationTier());
      assertDatanodeContainerAndBlock(keyInfo, storagePolicy.getCreationTier());
    }
  }

  /**
   * A file created without a storage policy inherits the bucket's policy, and
   * without a bucket policy either it gets the cluster default.
   */
  private void assertFileInheritsBucketPolicyOrClusterDefault(BucketLayout bucketLayout)
      throws IOException {
    OzoneStoragePolicy bucketPolicy = OzoneStoragePolicy.COLD;
    OmKeyInfo inherited = createFileAndLookup(createBucket(bucketPolicy, bucketLayout), null);
    assertKeyInfo(inherited, bucketPolicy);
    assertScmContainerAndPipeline(inherited, bucketPolicy.getCreationTier());
    assertDatanodeContainerAndBlock(inherited, bucketPolicy.getCreationTier());

    OzoneStoragePolicy defaultPolicy = OzoneStoragePolicy.getDefaultPolicy();
    OmKeyInfo defaulted = createFileAndLookup(createBucket(null, bucketLayout), null);
    assertKeyInfo(defaulted, defaultPolicy);
    assertScmContainerAndPipeline(defaulted, defaultPolicy.getCreationTier());
    assertDatanodeContainerAndBlock(defaulted, defaultPolicy.getCreationTier());
  }

  /**
   * Tests two scenarios on a file that spans several blocks.
   *
   * <p>Multi-block write: Tests whether a file that needs more blocks than OM pre-allocates keeps its policy on
   * every block. Creating the file with size 0 makes OM pre-allocate one block, so the other blocks come from
   * allocateBlock calls. Every block, not just the first, must sit on the COLD tier.
   *
   * <p>Overwrite: Overwriting a file with a different storage policy updates the recorded
   * policy and moves the new blocks to the new tier.
   */
  private void assertMultiBlockFileAndOverwriteHonourPolicy(BucketLayout bucketLayout)
      throws IOException {
    OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, bucketLayout);
    String keyName = UUID.randomUUID().toString();

    writeFile(bucket, keyName, multiBlockData(), 0, false, OzoneStoragePolicy.COLD);
    OmKeyInfo original = lookupKey(bucket, keyName);
    assertThat(original.getStoragePolicy()).as("layout %s", bucketLayout).isEqualTo(OzoneStoragePolicy.COLD);
    int numOfBlocks = original.getLatestVersionLocations().getBlocksLatestVersionOnly().size();
    assertThat(numOfBlocks)
        .withFailMessage("Expected more than one block so a later allocation is exercised, "
            + "but the file in a %s bucket has %d", bucketLayout, numOfBlocks)
        .isGreaterThan(1);
    assertAllBlocksOnTier(original, OzoneStoragePolicy.COLD.getCreationTier());
    assertAllBlocksOnDatanodes(original, OzoneStoragePolicy.COLD.getCreationTier());

    writeFile(bucket, keyName, multiBlockData(), 0, true, OzoneStoragePolicy.HOT);
    OmKeyInfo overwritten = lookupKey(bucket, keyName);
    assertThat(overwritten.getStoragePolicy()).as("layout %s", bucketLayout).isEqualTo(OzoneStoragePolicy.HOT);
    assertAllBlocksOnTier(overwritten, OzoneStoragePolicy.HOT.getCreationTier());
    assertAllBlocksOnDatanodes(overwritten, OzoneStoragePolicy.HOT.getCreationTier());
  }

  /**
   * The storage policy survives the round trip to the client on read.
   */
  @Test
  public void testStoragePolicyVisibleToClient() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
    String keyName = UUID.randomUUID().toString();
    writeKey(bucket, keyName, OzoneStoragePolicy.COLD);

    OzoneKeyDetails details = bucket.getKey(keyName);
    assertEquals(OzoneStoragePolicy.COLD, details.getStoragePolicy());
  }

  /**
   * A key written through an FSO bucket carries its storage policy too, since FSO
   * creates keys through a separate OM request path.
   */
  @Test
  public void testStoragePolicyOnFsoBucket() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket =
        createBucket(null, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OmKeyInfo keyInfo = createKeyAndLookup(bucket, OzoneStoragePolicy.HOT);

    assertEquals(keyInfo.getDataSize(), policyBytes(OzoneStoragePolicy.HOT, StorageTier.SSD, "preferred"));
    assertKeyInfo(keyInfo, OzoneStoragePolicy.HOT);
    assertScmContainerAndPipeline(keyInfo,
        OzoneStoragePolicy.HOT.getCreationTier());
    assertDatanodeContainerAndBlock(keyInfo,
        OzoneStoragePolicy.HOT.getCreationTier());
  }

  /**
   * An empty key skips block allocation but must still record its policy,
   * otherwise a later rewrite would lose it.
   */
  @Test
  public void testEmptyKeyRecordsStoragePolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
    String keyName = UUID.randomUUID().toString();
    try (OzoneOutputStream out = bucket.createKey(keyName, CreateKeyOptions.builder()
        .setReplicationConfig(RATIS_THREE)
        .setStoragePolicy(OzoneStoragePolicy.COLD)
        .build())) {
      // Intentionally write nothing.
      assertNotNull(out);
    }

    OmKeyInfo keyInfo = lookupKey(bucket, keyName);
    assertEquals(OzoneStoragePolicy.COLD, keyInfo.getStoragePolicy());
    assertTrue(keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly().isEmpty());
    assertEquals(0, policyBytes(OzoneStoragePolicy.COLD, StorageTier.ARCHIVE, "preferred"));
  }

  /**
   * Blocks allocated after the pre-allocated ones run out must stay on the key's tier.
   *
   * <p>Creating the key with size 0 pre-allocates nothing, so every block comes from a separate
   * allocateBlock call. The key asks for COLD while its bucket is WARM, so if the policy is not
   * carried into those calls, OM falls back to the bucket and the later blocks land on DISK
   * instead of ARCHIVE.
   */
  @Test
  public void testStoragePolicyHonouredOnSubsequentBlockAllocation()
      throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket =
        createBucket(OzoneStoragePolicy.WARM, BucketLayout.OBJECT_STORE);
    String keyName = UUID.randomUUID().toString();

    // Size 0 means OM pre-allocates no blocks, so the first write already needs an
    // allocateBlock round trip. Write enough to span more than one block.
    byte[] data = new byte[(int) (2 * ozoneManager.getScmBlockSize() + 1024)];
    Arrays.fill(data, (byte) 'p');
    try (OzoneOutputStream out = bucket.createKey(keyName, CreateKeyOptions.builder()
        .setReplicationConfig(RATIS_THREE)
        .setStoragePolicy(OzoneStoragePolicy.COLD)
        .build())) {
      out.write(data);
    }

    OmKeyInfo keyInfo = lookupKey(bucket, keyName);
    assertEquals(OzoneStoragePolicy.COLD, keyInfo.getStoragePolicy());

    List<OmKeyLocationInfo> blocks =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    assertThat(blocks.size())
        .withFailMessage("Expected more than one block so a later allocation is exercised, "
            + "but the key has %d", blocks.size())
        .isGreaterThan(1);

    // Every block, not just the first, must sit on the tier the key asked for.
    assertAllBlocksOnTier(keyInfo, OzoneStoragePolicy.COLD.getCreationTier());
    assertEquals(data.length, policyBytes(OzoneStoragePolicy.COLD, StorageTier.ARCHIVE, "preferred"));
  }

  /**
   * Overwriting a key with a different storage policy must update the recorded policy, not just
   * the tier of the new blocks.
   *
   * <p>The overwrite takes the existing-key branch of {@code prepareFileInfo}, which starts from
   * a copy of the old key. If that branch does not apply the new policy, the blocks land on the new
   * tier while {@code getStoragePolicy()} keeps reporting the old one.
   */
  @Test
  public void testOverwriteWithDifferentStoragePolicyUpdatesKey()
      throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
    String keyName = UUID.randomUUID().toString();

    writeKey(bucket, keyName, OzoneStoragePolicy.HOT);
    OmKeyInfo original = lookupKey(bucket, keyName);
    assertEquals(OzoneStoragePolicy.HOT, original.getStoragePolicy());
    assertAllBlocksOnTier(original, OzoneStoragePolicy.HOT.getCreationTier());

    // Overwrite the same key name with a different policy.
    writeKey(bucket, keyName, OzoneStoragePolicy.COLD);
    OmKeyInfo overwritten = lookupKey(bucket, keyName);

    // The recorded policy and the tier the data actually sits on must agree.
    assertEquals(OzoneStoragePolicy.COLD, overwritten.getStoragePolicy());
    assertAllBlocksOnTier(overwritten, OzoneStoragePolicy.COLD.getCreationTier());
    assertEquals(original.getDataSize(), policyBytes(OzoneStoragePolicy.HOT, StorageTier.SSD, "preferred"));
    assertEquals(overwritten.getDataSize(), policyBytes(OzoneStoragePolicy.COLD, StorageTier.ARCHIVE, "preferred"));
  }

  /**
   * A multipart key honours its storage policy: it inherits the bucket policy unless the upload is
   * initiated with one, and every block of every part lands on the tier of that policy.
   *
   * <p>No block is allocated when a part is opened. The client allocates them one by one while writing,
   * sending the storage policy of the part open key with each call, and OM falls back to the bucket
   * policy when none is sent. So the part open key must carry the policy of the multipart key. The
   * COLD upload on a WARM bucket is what catches a missing policy: with the fallback, its blocks would
   * land on the WARM tier instead of the COLD one. The first part spans two blocks so that more than
   * one allocateBlock call is checked.
   */
  @Test
  public void testMultipartUploadStoragePolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    for (BucketLayout layout : Arrays.asList(BucketLayout.OBJECT_STORE, BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, layout);

      assertMultipartKey(doMultipartUpload(bucket, UUID.randomUUID().toString(), null),
          OzoneStoragePolicy.WARM);
      assertMultipartKey(doMultipartUpload(bucket, UUID.randomUUID().toString(), OzoneStoragePolicy.COLD),
          OzoneStoragePolicy.COLD);
    }
  }

  /**
   * EC keys and files honour their storage policy, for every bucket layout. One cluster is shared by all the
   * checks, because each check uses its own bucket.
   */
  @Test
  public void testEcStoragePolicy() throws Exception {
    startCluster(allTierTopology(EC_RS_3_2.getRequiredNodes()));
    for (BucketLayout layout : new BucketLayout[] {BucketLayout.OBJECT_STORE, BucketLayout.FILE_SYSTEM_OPTIMIZED}) {
      verifyEcKeyStoragePolicy(layout);
      verifyEcMultipartUploadStoragePolicy(layout);
    }
    for (BucketLayout layout : FILE_BUCKET_LAYOUTS) {
      verifyEcFileStoragePolicy(layout);
    }
  }

  /**
   * An EC key honours its storage policy end to end: the block group is allocated on a pipeline and container
   * of the policy's creation tier, and all the cells of the block group (data and parity) land on datanode
   * volumes of that tier.
   */
  private void verifyEcKeyStoragePolicy(BucketLayout layout) throws Exception {
    byte[] data = ecStripeData();

    for (OzoneStoragePolicy storagePolicy : OzoneStoragePolicy.values()) {
      OzoneBucket bucket = createBucket(null, layout);
      String keyName = UUID.randomUUID().toString();
      writeKey(bucket, keyName, storagePolicy, EC_RS_3_2, data);
      assertEcKeyOnTier(lookupKey(bucket, keyName), storagePolicy);
    }
  }

  /**
   * An EC file created through createFile honours its storage policy in the same way as an EC key created
   * through createKey.
   */
  private void verifyEcFileStoragePolicy(BucketLayout layout) throws Exception {
    byte[] data = ecStripeData();

    for (OzoneStoragePolicy storagePolicy : OzoneStoragePolicy.values()) {
      OzoneBucket bucket = createBucket(null, layout);
      String keyName = UUID.randomUUID().toString();
      writeFile(bucket, keyName, data, data.length, false, storagePolicy, EC_RS_3_2);
      assertEcKeyOnTier(lookupKey(bucket, keyName), storagePolicy);
    }
  }

  /** One full stripe, so that every datanode of the block group receives a cell. */
  private static byte[] ecStripeData() {
    byte[] data = new byte[EC_RS_3_2.getData() * EC_RS_3_2.getEcChunkSize()];
    Arrays.fill(data, (byte) 'e');
    return data;
  }

  private void assertEcKeyOnTier(OmKeyInfo keyInfo, OzoneStoragePolicy storagePolicy) throws IOException {
    assertKeyInfo(keyInfo, storagePolicy, EC_RS_3_2);
    assertScmContainerAndPipeline(keyInfo, storagePolicy.getCreationTier(), EC_RS_3_2);
    assertEcDatanodeContainers(keyInfo, storagePolicy.getCreationTier(), EC_RS_3_2.getRequiredNodes());
  }

  /**
   * An EC multipart key honours its storage policy: it inherits the bucket policy unless the upload is
   * initiated with one, and every block group of every part lands on the tier of that policy.
   */
  private void verifyEcMultipartUploadStoragePolicy(BucketLayout layout) throws Exception {
    OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, layout);

    assertEcMultipartKey(doMultipartUpload(bucket, UUID.randomUUID().toString(), null, EC_RS_3_2),
        OzoneStoragePolicy.WARM);
    assertEcMultipartKey(doMultipartUpload(bucket, UUID.randomUUID().toString(), OzoneStoragePolicy.COLD, EC_RS_3_2),
        OzoneStoragePolicy.COLD);
  }

  /**
   * The policy of a multipart key is fixed when the upload is initiated, so changing the bucket policy
   * while the upload is open does not move the blocks of the parts uploaded afterwards.
   */
  @ParameterizedTest
  @EnumSource(value = BucketLayout.class, names = {"OBJECT_STORE", "FILE_SYSTEM_OPTIMIZED"})
  public void testMultipartUploadKeepsStoragePolicyOfInitiate(BucketLayout layout) throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    for (OzoneStoragePolicy changedPolicy : Arrays.asList(OzoneStoragePolicy.HOT, OzoneStoragePolicy.COLD)) {
      OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, layout);
      String keyName = UUID.randomUUID().toString();
      String uploadID = initiateMultipartUpload(bucket, keyName, null);
      bucket.setStoragePolicy(changedPolicy);
      uploadPartsAndComplete(bucket, keyName, uploadID);

      assertMultipartKey(lookupKey(bucket, keyName), OzoneStoragePolicy.WARM);
    }
  }

  /**
   * Overwriting a key must leave it with the storage policy of the write that produced its blocks,
   * not the policy of the key it replaced, the same rule as for a plain key overwrite.
   */
  @Test
  public void testMultipartUploadOverwriteWithDifferentStoragePolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
    String keyName = UUID.randomUUID().toString();

    writeKey(bucket, keyName, OzoneStoragePolicy.HOT);
    assertThat(lookupKey(bucket, keyName).getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);

    assertMultipartKey(doMultipartUpload(bucket, keyName, OzoneStoragePolicy.COLD), OzoneStoragePolicy.COLD);
    assertMultipartKey(doMultipartUpload(bucket, keyName, OzoneStoragePolicy.HOT), OzoneStoragePolicy.HOT);

    writeKey(bucket, keyName, OzoneStoragePolicy.COLD);
    OmKeyInfo plainKey = lookupKey(bucket, keyName);
    assertThat(plainKey.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.COLD);
    assertAllBlocksOnTier(plainKey, OzoneStoragePolicy.COLD.getCreationTier());

    String hotUpload = initiateMultipartUpload(bucket, keyName, OzoneStoragePolicy.HOT);
    String coldUpload = initiateMultipartUpload(bucket, keyName, OzoneStoragePolicy.COLD);
    uploadPartsAndComplete(bucket, keyName, hotUpload);
    assertMultipartKey(lookupKey(bucket, keyName), OzoneStoragePolicy.HOT);
    uploadPartsAndComplete(bucket, keyName, coldUpload);
    assertMultipartKey(lookupKey(bucket, keyName), OzoneStoragePolicy.COLD);
  }

  /**
   * {@code storagepolicy check} must report YES for a key whose replicas sit on the policy's creation
   * tier, using the real OM key locations and the real SCM replica reports.
   */
  @Test
  public void testCheckReportsMatchForEveryPolicy() throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    try (ScmClient scmClient = new ContainerOperationClient(cluster.getConf())) {
      for (OzoneStoragePolicy storagePolicy : OzoneStoragePolicy.values()) {
        OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
        String keyName = UUID.randomUUID().toString();
        writeKey(bucket, keyName, storagePolicy);
        OzoneKeyDetails keyDetails = bucket.getKey(keyName);
        waitForReplicaReports(scmClient, keyDetails);

        String report = new CheckStoragePolicySubCommand().buildReport(keyDetails, scmClient, true);

        assertThat(report).as("report for %s", storagePolicy)
            .contains("  Key Match Storage Policy: YES")
            .doesNotContain("Replica Match: NO");
        assertThat(StringUtils.countMatches(report, "Replica Match: YES"))
            .isEqualTo(RATIS_THREE.getRequiredNodes());
      }
    }
  }

  /**
   * A HOT key on a cluster without SSD volumes falls back to DISK (the bucket allows fallback by
   * default). The strict rule must report that as NO and name the volume type.
   */
  @Test
  public void testCheckReportsMismatchWhenKeyFellBackToFallbackTier() throws Exception {
    startCluster(Collections.nCopies(RATIS_THREE.getRequiredNodes(),
        Arrays.asList(StorageType.DISK, StorageType.ARCHIVE)));

    try (ScmClient scmClient = new ContainerOperationClient(cluster.getConf())) {
      OzoneBucket bucket = createBucket(null, BucketLayout.OBJECT_STORE);
      String keyName = UUID.randomUUID().toString();
      writeKey(bucket, keyName, OzoneStoragePolicy.HOT);
      assertEquals(lookupKey(bucket, keyName).getDataSize(),
          policyBytes(OzoneStoragePolicy.HOT, StorageTier.DISK, "fallback"));
      assertEquals(0, policyBytes(OzoneStoragePolicy.HOT, StorageTier.SSD, "preferred"));
      OzoneKeyDetails keyDetails = bucket.getKey(keyName);
      waitForReplicaReports(scmClient, keyDetails);

      String report = new CheckStoragePolicySubCommand().buildReport(keyDetails, scmClient, true);

      assertThat(report)
          .contains("  Key Match Storage Policy: NO")
          .contains("volumeStorageType DISK != expected SSD")
          .doesNotContain("Replica Match: YES");
    }
  }

  /**
   * Waits until SCM knows every replica of every container of the key, including the storage types
   * the datanodes report in their heartbeats.
   */
  private void waitForReplicaReports(ScmClient scmClient, OzoneKeyDetails keyDetails) throws Exception {
    for (OzoneKeyLocation location : keyDetails.getOzoneKeyLocations()) {
      GenericTestUtils.waitFor((BooleanSupplier) () -> {
        try {
          List<ContainerReplicaInfo> replicas = scmClient.getContainerReplicas(location.getContainerID());
          return replicas.size() == RATIS_THREE.getRequiredNodes()
              && replicas.stream().allMatch(r -> r.getStorageType() != null && r.getVolumeStorageType() != null);
        } catch (IOException e) {
          return false;
        }
      }, 500, 30000);
    }
  }

  /**
   * An older client does not send the storage policy in allocateBlock. If the bucket policy changes
   * while such a client writes a key, the next blocks must stay on the tier of the key's policy.
   *
   * <p>The current client always sends the policy, so this test calls the OM protocol directly and
   * leaves the policy out of {@link OmKeyArgs}, as an older client does. The key opens while the
   * bucket policy is HOT. The bucket policy then changes to COLD. If OM resolved the policy from
   * the bucket, the next block would go to the ARCHIVE tier instead of the SSD tier.
   */
  @Test
  public void testOldClientAllocateBlockKeepsKeyPolicyWhenBucketPolicyChanges()
      throws Exception {
    startCluster(allTierTopology(RATIS_THREE.getRequiredNodes()));

    OzoneBucket bucket =
        createBucket(OzoneStoragePolicy.HOT, BucketLayout.OBJECT_STORE);
    OzoneManagerProtocol omClient = store.getClientProxy().getOzoneManagerClient();
    OmKeyArgs oldClientKeyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(UUID.randomUUID().toString())
        .setReplicationConfig(RATIS_THREE)
        .setDataSize(1)
        .setAcls(new ArrayList<>())
        .setOwnerName("ownerName")
        .build();

    // The older client opens the key. The size is not 0, so OM pre-allocates the first block.
    OpenKeySession session = omClient.openKey(oldClientKeyArgs);
    assertThat(session.getKeyInfo().getStoragePolicy()).isEqualTo(OzoneStoragePolicy.HOT);
    List<OmKeyLocationInfo> initialBlocks =
        session.getKeyInfo().getLatestVersionLocations().getBlocksLatestVersionOnly();
    assertThat(initialBlocks).hasSize(1);
    assertBlockOnTier(initialBlocks.get(0), OzoneStoragePolicy.HOT.getCreationTier());

    // The bucket policy changes while the key is still open.
    bucket.setStoragePolicy(OzoneStoragePolicy.COLD);
    assertThat(store.getVolume(bucket.getVolumeName()).getBucket(bucket.getName()).getStoragePolicy())
        .isEqualTo(OzoneStoragePolicy.COLD);

    // The older client asks for the next block, again without a policy.
    OmKeyLocationInfo nextBlock =
        omClient.allocateBlock(oldClientKeyArgs, session.getId(), new ExcludeList());

    // The block must follow the key's policy (HOT), not the new bucket policy (COLD).
    assertBlockOnTier(nextBlock, OzoneStoragePolicy.HOT.getCreationTier());
  }

  private void assertBlockOnTier(OmKeyLocationInfo block, StorageTier expectedTier)
      throws IOException {
    ContainerInfo containerInfo = containerManager.getContainer(
        ContainerID.valueOf(block.getContainerID()));
    assertThat(containerInfo.getStorageTier().getUniformStorageType())
        .as("Mismatch in storage tier of container %s, which holds the block", block.getContainerID())
        .isEqualTo(expectedTier.getUniformStorageType());
  }

  /**
   * Checks every block of a key landed on a container of the expected tier. This reads SCM's container record
   * only; to check the datanode volumes, use {@code assertAllBlocksOnDatanodes}.
   */
  private void assertAllBlocksOnTier(OmKeyInfo keyInfo, StorageTier expectedTier)
      throws IOException {
    StorageType expectedStorageType = expectedTier.getUniformStorageType();
    List<OmKeyLocationInfo> blocks =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    for (int i = 0; i < blocks.size(); i++) {
      OmKeyLocationInfo block = blocks.get(i);
      ContainerInfo containerInfo = containerManager.getContainer(
          ContainerID.valueOf(block.getContainerID()));
      assertEquals(expectedStorageType,
          containerInfo.getStorageTier().getUniformStorageType(),
          "block " + i + " of " + blocks.size() + " landed on the wrong tier");
    }
  }

  /**
   * Checks every block of a key on the datanodes, not just in SCM's records. Looping is only needed for
   * a file that spans several blocks; for the single-block files in the other createFile scenarios, the
   * one-argument {@code assertDatanodeContainerAndBlock} (block 0) is enough.
   */
  private void assertAllBlocksOnDatanodes(OmKeyInfo keyInfo, StorageTier expectedTier)
      throws IOException {
    int blockCount = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().size();
    for (int i = 0; i < blockCount; i++) {
      assertDatanodeContainerAndBlock(keyInfo, i, expectedTier);
    }
  }

  /** Initiates an upload, completes it with two parts and returns the completed key. */
  private OmKeyInfo doMultipartUpload(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy) throws Exception {
    return doMultipartUpload(bucket, keyName, storagePolicy, RATIS_THREE);
  }

  private OmKeyInfo doMultipartUpload(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy, ReplicationConfig replicationConfig) throws Exception {
    String uploadID = initiateMultipartUpload(bucket, keyName, storagePolicy, replicationConfig);
    uploadPartsAndComplete(bucket, keyName, uploadID);
    return lookupKey(bucket, keyName);
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy) throws IOException {
    return initiateMultipartUpload(bucket, keyName, storagePolicy, RATIS_THREE);
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy, ReplicationConfig replicationConfig) throws IOException {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        MultipartUploadOptions.builder().setReplicationConfig(replicationConfig).setStoragePolicy(storagePolicy)
            .build());
    assertThat(multipartInfo.getUploadID()).isNotNull();
    return multipartInfo.getUploadID();
  }

  /**
   * Uploads a part of the minimum size, which spans more than one block, and a small last part,
   * then completes the upload.
   */
  private void uploadPartsAndComplete(OzoneBucket bucket, String keyName, String uploadID)
      throws IOException {
    Map<Integer, String> partsMap = new TreeMap<>();
    int[] partSizes = {OzoneConsts.OM_MULTIPART_MIN_SIZE, 1024};
    for (int i = 0; i < partSizes.length; i++) {
      int partNumber = i + 1;
      byte[] data = new byte[partSizes[i]];
      Arrays.fill(data, (byte) ('a' + i));
      OzoneOutputStream out = bucket.createMultipartKey(keyName, data.length, partNumber, uploadID);
      out.write(data);
      out.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(data));
      out.close();
      partsMap.put(partNumber, out.getCommitUploadPartInfo().getETag());
    }
    bucket.completeMultipartUpload(keyName, uploadID, partsMap);
  }

  /** Checks the key records the policy and that all of its several blocks sit on the tier of that policy. */
  private void assertMultipartKey(OmKeyInfo keyInfo, OzoneStoragePolicy expectedPolicy)
      throws IOException {
    assertThat(keyInfo.getStoragePolicy()).isEqualTo(expectedPolicy);
    assertThat(keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().size())
        .withFailMessage("Expected more than one block so that several allocations are exercised")
        .isGreaterThan(1);
    assertAllBlocksOnTier(keyInfo, expectedPolicy.getCreationTier());
    assertDatanodeContainerAndBlock(keyInfo, expectedPolicy.getCreationTier());
  }

  /**
   * Same as {@link #assertMultipartKey(OmKeyInfo, OzoneStoragePolicy)} for an EC key. A part smaller than a
   * stripe leaves some datanodes without a replica, so only the first data replica and the parity replicas
   * are required on the datanodes.
   */
  private void assertEcMultipartKey(OmKeyInfo keyInfo, OzoneStoragePolicy expectedPolicy) throws IOException {
    assertThat(keyInfo.getStoragePolicy()).isEqualTo(expectedPolicy);
    assertThat(keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().size())
        .withFailMessage("Expected more than one block so that several allocations are exercised")
        .isGreaterThan(1);
    assertAllBlocksOnTier(keyInfo, expectedPolicy.getCreationTier());
    assertEcDatanodeContainers(keyInfo, expectedPolicy.getCreationTier(), 1 + EC_RS_3_2.getParity());
  }

  /**
   * Checks the replicas of every block group of an EC key: each replica container sits on a volume of the
   * expected tier, no replica index appears twice, and at least {@code minReplicas} replicas exist.
   * So all the cells of a block group are on the same storage type.
   */
  private void assertEcDatanodeContainers(OmKeyInfo keyInfo, StorageTier expectedTier, int minReplicas)
      throws IOException {
    StorageType expectedStorageType = expectedTier.getUniformStorageType();
    for (OmKeyLocationInfo block : keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly()) {
      Set<Integer> replicaIndexes = new HashSet<>();
      for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
        Container<?> container = datanode.getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(block.getContainerID());
        if (container == null) {
          continue;
        }
        ContainerData containerData = container.getContainerData();
        int replicaIndex = containerData.getReplicaIndex();
        assertThat(replicaIndexes.add(replicaIndex))
            .as("Replica index %s of container %s is on more than one datanode", replicaIndex,
                block.getContainerID())
            .isTrue();
        assertThat(containerData.getStorageType())
            .as("Replica %s of container %s is on the wrong storage type", replicaIndex, block.getContainerID())
            .isEqualTo(expectedStorageType);
        assertThat(containerData.getVolume().getStorageType()).isEqualTo(expectedStorageType);
        assertThat(ContainerDataYaml.readContainerFile(container.getContainerFile()).getStorageType())
            .isEqualTo(expectedStorageType);

        KeyValueContainerData kvData = (KeyValueContainerData) containerData;
        try (DBHandle db = BlockUtils.getDB(kvData, conf)) {
          // A replica holds the block only if one of the cells of the block group was written to it.
          BlockData storedBlock = db.getStore().getBlockDataTable().get(kvData.getBlockKey(block.getLocalID()));
          if (storedBlock != null) {
            assertThat(storedBlock.getBlockID().getStorageType())
                .as("Storage type must be stored on the container, not each block")
                .isNull();
          }
        }
      }
      assertThat(replicaIndexes).hasSizeGreaterThanOrEqualTo(minReplicas);
      assertThat(replicaIndexes).isSubsetOf(1, 2, 3, 4, 5);
    }
  }

  private OzoneBucket createBucket(OzoneStoragePolicy storagePolicy,
      BucketLayout bucketLayout) throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder()
        .setBucketLayout(bucketLayout)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(RATIS_THREE));
    if (storagePolicy != null) {
      builder.setStoragePolicy(storagePolicy);
    }
    store.getVolume(volumeName).createBucket(bucketName, builder.build());
    return store.getVolume(volumeName).getBucket(bucketName);
  }

  private long policyBytes(OzoneStoragePolicy policy, StorageTier actualTier, String outcome) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    DefaultMetricsSystem.instance().getSource("OMStoragePolicyMetrics").getMetrics(collector, true);
    long bytes = 0;
    for (MetricsRecordImpl record : collector.getRecords()) {
      Map<String, String> tags = record.tags().stream()
          .collect(Collectors.toMap(tag -> tag.name(), tag -> tag.value()));
      if (policy.name().equals(tags.get("Policy")) && actualTier.name().equals(tags.get("ActualTier"))
          && outcome.equals(tags.get("Outcome"))) {
        assertEquals(policy.getCreationTier().name(), tags.get("RequestedTier"));
        for (AbstractMetric metric : record.metrics()) {
          if ("CommittedBytes".equals(metric.name())) {
            bytes += metric.value().longValue();
          }
        }
      }
    }
    return bytes;
  }

  private void writeKey(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy) throws IOException {
    writeKey(bucket, keyName, storagePolicy, RATIS_THREE, UUID.randomUUID().toString().getBytes(UTF_8));
  }

  private void writeKey(OzoneBucket bucket, String keyName, OzoneStoragePolicy storagePolicy,
      ReplicationConfig replicationConfig, byte[] data) throws IOException {
    try (OzoneOutputStream out = bucket.createKey(keyName, CreateKeyOptions.builder()
        .setSize(data.length)
        .setReplicationConfig(replicationConfig)
        .setStoragePolicy(storagePolicy)
        .build())) {
      out.write(data);
    }
  }

  private OmKeyInfo createKeyAndLookup(OzoneBucket bucket,
      OzoneStoragePolicy storagePolicy) throws IOException {
    String keyName = UUID.randomUUID().toString();
    writeKey(bucket, keyName, storagePolicy);
    return lookupKey(bucket, keyName);
  }

  private void writeFile(OzoneBucket bucket, String keyName, byte[] data, long size, boolean overWrite,
      OzoneStoragePolicy storagePolicy) throws IOException {
    writeFile(bucket, keyName, data, size, overWrite, storagePolicy, RATIS_THREE);
  }

  private void writeFile(OzoneBucket bucket, String keyName, byte[] data, long size, boolean overWrite,
      OzoneStoragePolicy storagePolicy, ReplicationConfig replicationConfig) throws IOException {
    try (OzoneOutputStream out = bucket.createFile(keyName, CreateFileOptions.builder()
        .setSize(size)
        .setReplicationConfig(replicationConfig)
        .setOverWrite(overWrite)
        .setStoragePolicy(storagePolicy)
        .build())) {
      out.write(data);
    }
  }

  private OmKeyInfo createFileAndLookup(OzoneBucket bucket,
      OzoneStoragePolicy storagePolicy) throws IOException {
    String keyName = UUID.randomUUID().toString();
    byte[] data = UUID.randomUUID().toString().getBytes(UTF_8);
    writeFile(bucket, keyName, data, data.length, false, storagePolicy);
    return lookupKey(bucket, keyName);
  }

  /** Data that fills more than two blocks. */
  private byte[] multiBlockData() {
    byte[] data = new byte[(int) (2 * ozoneManager.getScmBlockSize() + 1024)];
    Arrays.fill(data, (byte) 'p');
    return data;
  }

  private OmKeyInfo lookupKey(OzoneBucket bucket, String keyName)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(bucket.getVolumeName())
        .setBucketName(bucket.getName())
        .setKeyName(keyName)
        .build();
    return ozoneManager.lookupKey(keyArgs);
  }

  private void assertKeyInfo(OmKeyInfo keyInfo,
      OzoneStoragePolicy expectedPolicy) {
    assertKeyInfo(keyInfo, expectedPolicy, RATIS_THREE);
  }

  private void assertKeyInfo(OmKeyInfo keyInfo,
      OzoneStoragePolicy expectedPolicy, ReplicationConfig replicationConfig) {
    assertEquals(1, keyInfo.getKeyLocationVersions().size());
    assertNotNull(keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly());
    assertEquals(expectedPolicy, keyInfo.getStoragePolicy());
    assertEquals(replicationConfig, keyInfo.getReplicationConfig());
  }

  private void assertScmContainerAndPipeline(OmKeyInfo keyInfo,
      StorageTier expectedTier) throws IOException {
    assertScmContainerAndPipeline(keyInfo, expectedTier, RATIS_THREE);
  }

  private void assertScmContainerAndPipeline(OmKeyInfo keyInfo,
      StorageTier expectedTier, ReplicationConfig replicationConfig) throws IOException {
    List<OmKeyLocationInfo> blocks =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    if (blocks.isEmpty()) {
      return;
    }
    ContainerInfo containerInfo = containerManager.getContainer(
        ContainerID.valueOf(blocks.get(0).getContainerID()));
    Pipeline pipeline =
        pipelineManager.getPipeline(containerInfo.getPipelineID());

    assertEquals(replicationConfig, pipeline.getReplicationConfig());
    assertEquals(expectedTier, containerInfo.getStorageTier());
    assertEquals(expectedTier, pipeline.getSupportedStorageTier());
    assertEquals(replicationConfig.getRequiredNodes(), pipeline.getNodeSet().size());
  }

  /**
   * Checks the first block of a key on the datanodes. Meant for single-block keys; for a key that spans
   * several blocks, check each block with {@link #assertDatanodeContainerAndBlock(OmKeyInfo, int, StorageTier)}.
   */
  private void assertDatanodeContainerAndBlock(OmKeyInfo keyInfo,
      StorageTier expectedTier) throws IOException {
    assertDatanodeContainerAndBlock(keyInfo, 0, expectedTier);
  }

  /**
   * Checks the block at {@code blockIndex} actually landed on a volume of the expected tier.
   */
  private void assertDatanodeContainerAndBlock(OmKeyInfo keyInfo, int blockIndex,
      StorageTier expectedTier) throws IOException {
    StorageType expectedStorageType = expectedTier.getUniformStorageType();
    OmKeyLocationInfo block = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(blockIndex);
    ContainerInfo containerInfo = containerManager.getContainer(
        ContainerID.valueOf(block.getContainerID()));
    Pipeline pipeline =
        pipelineManager.getPipeline(containerInfo.getPipelineID());

    int replicasChecked = 0;
    try (XceiverClientGrpc client = new XceiverClientGrpc(pipeline, conf)) {
      for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
        Container<?> container = datanode.getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(block.getContainerID());
        if (container == null) {
          continue;
        }
        ContainerData containerData = container.getContainerData();
        assertEquals(expectedStorageType, containerData.getStorageType());
        assertEquals(expectedStorageType,
            containerData.getVolume().getStorageType());
        assertEquals(expectedStorageType,
            ContainerDataYaml.readContainerFile(container.getContainerFile()).getStorageType());
        assertTrue(pipeline.getNodeSet().contains(datanode.getDatanodeDetails()));

        KeyValueContainerData kvData = (KeyValueContainerData) containerData;
        try (DBHandle db = BlockUtils.getDB(kvData, conf)) {
          assertNull(db.getStore().getBlockDataTable().get(kvData.getBlockKey(block.getLocalID()))
              .getBlockID().getStorageType(), "Storage type must be stored on the container, not each block");
        }
        ContainerProtos.BlockData blockData = ContainerProtocolCalls.getBlock(
            client, block.getBlockID(), null, client.getPipeline()).getBlockData();
        assertTrue(blockData.getBlockID().hasStorageTypeID());
        assertEquals(expectedStorageType, StorageTypeUtils
            .getStorageTypeFromID(blockData.getBlockID().getStorageTypeID()));
        replicasChecked++;
      }
    }
    assertEquals(RATIS_THREE.getRequiredNodes(), replicasChecked);
  }
}
