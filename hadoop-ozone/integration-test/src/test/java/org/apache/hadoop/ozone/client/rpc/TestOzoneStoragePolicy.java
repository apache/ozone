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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientGrpc;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentCaptor;

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
    OzoneManagerProtocol omClientSpy = spyOmClient();

    for (BucketLayout bucketLayout : FILE_BUCKET_LAYOUTS) {
      assertFileStoragePolicyIsHonoured(bucketLayout);
      assertFileInheritsBucketPolicyOrClusterDefault(bucketLayout);
      assertMultiBlockFileKeepsPolicy(bucketLayout, omClientSpy);
      assertOverwriteUpdatesPolicyAndPlacement(bucketLayout);
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
   * Tests whether a file that needs more blocks than OM pre-allocates keeps its policy on
   * every block, and the client sends that policy with every later allocateBlock call.
   *
   * <p>The file asks for COLD while its bucket is WARM, so a policy that is lost
   * on the way makes the later blocks land on DISK, or makes the call carry none.
   * Creating the file with size 0 makes OM pre-allocate one block, so the others
   * come from allocateBlock calls.
   */
  private void assertMultiBlockFileKeepsPolicy(BucketLayout bucketLayout, OzoneManagerProtocol omClientSpy)
      throws IOException {
    OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, bucketLayout);
    String keyName = UUID.randomUUID().toString();
    clearInvocations(omClientSpy);

    writeFile(bucket, keyName, multiBlockData(), 0, false, OzoneStoragePolicy.COLD);

    OmKeyInfo keyInfo = lookupKey(bucket, keyName);
    assertThat(keyInfo.getStoragePolicy()).as("layout %s", bucketLayout).isEqualTo(OzoneStoragePolicy.COLD);
    List<OmKeyLocationInfo> blocks = keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    assertThat(blocks.size())
        .withFailMessage("Expected more than one block so a later allocation is exercised, "
            + "but the file in a %s bucket has %d", bucketLayout, blocks.size())
        .isGreaterThan(1);

    ArgumentCaptor<OmKeyArgs> allocateBlockArgs = ArgumentCaptor.forClass(OmKeyArgs.class);
    verify(omClientSpy, atLeastOnce()).allocateBlock(allocateBlockArgs.capture(), anyLong(), any());
    List<StoragePolicy> sentPolicies = allocateBlockArgs.getAllValues().stream()
        .map(OmKeyArgs::getStoragePolicy)
        .collect(Collectors.toList());
    assertThat(sentPolicies)
        .as("storage policy in each allocateBlock call for a file in a %s bucket", bucketLayout)
        .isEqualTo(Collections.nCopies(blocks.size() - 1, OzoneStoragePolicy.COLD));

    // Every block, not just the first, must sit on the tier the file asked for.
    assertAllBlocksOnTier(keyInfo, OzoneStoragePolicy.COLD.getCreationTier());
  }

  /**
   * Overwriting a file with a different storage policy updates the recorded
   * policy and moves the new blocks to the new tier.
   */
  private void assertOverwriteUpdatesPolicyAndPlacement(BucketLayout bucketLayout)
      throws IOException {
    OzoneBucket bucket = createBucket(OzoneStoragePolicy.WARM, bucketLayout);
    String keyName = UUID.randomUUID().toString();

    writeFile(bucket, keyName, multiBlockData(), 0, false, OzoneStoragePolicy.COLD);
    OmKeyInfo original = lookupKey(bucket, keyName);
    assertThat(original.getStoragePolicy()).as("layout %s", bucketLayout).isEqualTo(OzoneStoragePolicy.COLD);
    assertAllBlocksOnTier(original, OzoneStoragePolicy.COLD.getCreationTier());

    writeFile(bucket, keyName, multiBlockData(), 0, true, OzoneStoragePolicy.HOT);
    OmKeyInfo overwritten = lookupKey(bucket, keyName);
    assertThat(overwritten.getStoragePolicy()).as("layout %s", bucketLayout).isEqualTo(OzoneStoragePolicy.HOT);
    assertAllBlocksOnTier(overwritten, OzoneStoragePolicy.HOT.getCreationTier());
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
    try (OzoneOutputStream out = bucket.createKey(keyName, 0, RATIS_THREE,
        Collections.emptyMap(), Collections.emptyMap(),
        OzoneStoragePolicy.COLD)) {
      // Intentionally write nothing.
      assertNotNull(out);
    }

    OmKeyInfo keyInfo = lookupKey(bucket, keyName);
    assertEquals(OzoneStoragePolicy.COLD, keyInfo.getStoragePolicy());
    assertTrue(keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly().isEmpty());
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
    try (OzoneOutputStream out = bucket.createKey(keyName, 0, RATIS_THREE,
        Collections.emptyMap(), Collections.emptyMap(),
        OzoneStoragePolicy.COLD)) {
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
  }

  /**
   * Checks every block of a key landed on a container of the expected tier. The
   * {@code assertDatanodeContainerAndBlock} helper only inspects the first block, which cannot
   * catch a tier that changes partway through a key.
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

  private void writeKey(OzoneBucket bucket, String keyName,
      OzoneStoragePolicy storagePolicy) throws IOException {
    byte[] data = UUID.randomUUID().toString().getBytes(UTF_8);
    try (OzoneOutputStream out = bucket.createKey(keyName, data.length,
        RATIS_THREE, Collections.emptyMap(), Collections.emptyMap(),
        storagePolicy)) {
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
    try (OzoneOutputStream out = bucket.createFile(keyName, size,
        RATIS_THREE, overWrite, false, storagePolicy)) {
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

  /**
   * Replaces the client's OM protocol with a mock that delegates to the real one,
   * so a test can inspect the calls the client sends to OM.
   */
  private OzoneManagerProtocol spyOmClient() {
    RpcClient rpcClient = (RpcClient) ozClient.getProxy();
    OzoneManagerClientProtocol omClient = (OzoneManagerClientProtocol) rpcClient.getOzoneManagerClient();
    OzoneManagerClientProtocol omClientSpy =
        mock(OzoneManagerClientProtocol.class, AdditionalAnswers.delegatesTo(omClient));
    HddsWhiteboxTestUtils.setInternalState(rpcClient, "ozoneManagerClient", omClientSpy);
    return omClientSpy;
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
    assertEquals(1, keyInfo.getKeyLocationVersions().size());
    assertNotNull(keyInfo.getLatestVersionLocations()
        .getBlocksLatestVersionOnly());
    assertEquals(expectedPolicy, keyInfo.getStoragePolicy());
    assertEquals(RATIS_THREE, keyInfo.getReplicationConfig());
  }

  private void assertScmContainerAndPipeline(OmKeyInfo keyInfo,
      StorageTier expectedTier) throws IOException {
    List<OmKeyLocationInfo> blocks =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    if (blocks.isEmpty()) {
      return;
    }
    ContainerInfo containerInfo = containerManager.getContainer(
        ContainerID.valueOf(blocks.get(0).getContainerID()));
    Pipeline pipeline =
        pipelineManager.getPipeline(containerInfo.getPipelineID());

    assertEquals(RATIS_THREE, pipeline.getReplicationConfig());
    assertEquals(expectedTier, containerInfo.getStorageTier());
    assertEquals(expectedTier, pipeline.getSupportedStorageTier());
    assertEquals(RATIS_THREE.getRequiredNodes(), pipeline.getNodeSet().size());
  }

  /**
   * Checks the block actually landed on a volume of the expected tier, and that
   * the storage type travelled to the datanode on the block id.
   */
  private void assertDatanodeContainerAndBlock(OmKeyInfo keyInfo,
      StorageTier expectedTier) throws IOException {
    StorageType expectedStorageType = expectedTier.getUniformStorageType();
    OmKeyLocationInfo block =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
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
        assertTrue(pipeline.getNodeSet().contains(datanode.getDatanodeDetails()));

        ContainerProtos.BlockData blockData = ContainerProtocolCalls.getBlock(
            client, block.getBlockID(), null, client.getPipeline()).getBlockData();
        assertEquals(expectedStorageType, StorageTypeUtils
            .getStorageTypeFromID(blockData.getBlockID().getStorageTypeID()));
        replicasChecked++;
      }
    }
    assertEquals(RATIS_THREE.getRequiredNodes(), replicasChecked);
  }
}
