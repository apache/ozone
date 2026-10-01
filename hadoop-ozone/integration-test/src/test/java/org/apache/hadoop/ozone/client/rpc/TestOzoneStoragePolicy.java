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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
            client, block.getBlockID(), null,
            client.getPipeline().getReplicaIndexes()).getBlockData();
        assertEquals(expectedStorageType, StorageTypeUtils
            .getStorageTypeFromID(blockData.getBlockID().getStorageTypeID()));
        replicasChecked++;
      }
    }
    assertEquals(RATIS_THREE.getRequiredNodes(), replicasChecked);
  }
}
