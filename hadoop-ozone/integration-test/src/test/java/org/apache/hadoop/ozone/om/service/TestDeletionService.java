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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.DeletedBlock;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * DeletionService test to Pass Usage from OM to SCM.
 */
public class TestDeletionService {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String KEY_NAME = "testkey";
  private static final int KEY_SIZE = 5 * 1024; // 5 KB

  public static Stream<Arguments> replicaType() {
    return Stream.of(
        arguments("RATIS", "ONE"),
        arguments("RATIS", "THREE")
    );
  }

  public static Stream<Arguments> ecType() {
    return Stream.of(
        arguments(ECReplicationConfig.EcCodec.RS, 3, 2, MB),
        arguments(ECReplicationConfig.EcCodec.RS, 3, 2, 2 * MB),
        arguments(ECReplicationConfig.EcCodec.RS, 6, 3, MB),
        arguments(ECReplicationConfig.EcCodec.RS, 6, 3, 2 * MB)
    );
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  public void testDeletedKeyBytesPropagatedToSCM(String type, String factor) throws Exception {
    OzoneConfiguration conf = createBasicConfig();
    MiniOzoneCluster cluster = null;

    try {
      // Step 1: Start MiniOzoneCluster
      cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
      cluster.waitForClusterToBeReady();

      ReplicationConfig replicationConfig = RatisReplicationConfig
          .getInstance(ReplicationFactor.valueOf(factor).toProto());

      // Step 2: Create volume, bucket, and write a key
      OzoneBucket bucket = setupClientAndWriteKey(cluster, type, factor, replicationConfig);
      // Step 3: Spy on BlockManager and inject it into SCM
      BlockManager spyManager = injectSpyBlockManager(cluster);
      // Step 4: Delete the key (which triggers deleteBlocks call)
      bucket.deleteKey(KEY_NAME);
      // Step 5: Verify deleteBlocks call and capture argument
      verifyAndAssertQuota(spyManager, replicationConfig);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void testGetDefaultShouldCreateECReplicationConfFromConfValues(
      ECReplicationConfig.EcCodec codec, int data, int parity, long chunkSize) throws IOException {

    OzoneConfiguration conf = createBasicConfig();
    MiniOzoneCluster cluster = null;

    try {
      ReplicationConfig replicationConfig = new ECReplicationConfig(data, parity, codec, (int) chunkSize);
      // Step 1: Start MiniOzoneCluster
      cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(data + parity).build();
      cluster.waitForClusterToBeReady();
      // Step 2: Create volume, bucket, and write a key
      OzoneBucket bucket = setupClientAndWriteKey(cluster, "EC", null, replicationConfig);
      // Step 3: Spy on BlockManager and inject it into SCM
      BlockManager spyManager = injectSpyBlockManager(cluster);
      // Step 4: Delete the key (which triggers deleteBlocks call)
      bucket.deleteKey(KEY_NAME);
      // Step 5: Verify deleteBlocks call and capture argument
      verifyAndAssertQuota(spyManager, replicationConfig);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private OzoneConfiguration createBasicConfig() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    return conf;
  }

  private OzoneBucket setupClientAndWriteKey(MiniOzoneCluster cluster, String type,
                                             String factor, ReplicationConfig replicationConfig)
      throws IOException {
    OzoneClient client = cluster.newClient();
    client.getObjectStore().createVolume(VOLUME_NAME);
    client.getObjectStore().getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    OzoneBucket bucket = client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);

    byte[] data = new byte[KEY_SIZE];
    try (OzoneOutputStream out = bucket.createKey(KEY_NAME, KEY_SIZE,
        replicationConfig, new HashMap<>())) {
      out.write(data);
    }
    return bucket;
  }

  private BlockManager injectSpyBlockManager(MiniOzoneCluster cluster) throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    BlockManager realManager = scm.getScmBlockManager();
    BlockManager spyManager = spy(realManager);

    Field field = scm.getClass().getDeclaredField("scmBlockManager");
    field.setAccessible(true);
    field.set(scm, spyManager);
    return spyManager;
  }

  private void verifyAndAssertQuota(BlockManager spyManager, ReplicationConfig replicationConfig) throws IOException {
    ArgumentCaptor<List<BlockGroup>> captor = ArgumentCaptor.forClass(List.class);
    verify(spyManager, timeout(50000).atLeastOnce()).deleteBlocks(captor.capture());

    long totalUsedBytes = captor.getAllValues().get(0).stream()
        .flatMap(group -> group.getAllBlocks().stream())
        .mapToLong(DeletedBlock::getReplicatedSize).sum();

    long totalUnreplicatedBytes = captor.getAllValues().get(0).stream()
        .flatMap(group -> group.getAllBlocks().stream())
        .mapToLong(DeletedBlock::getSize).sum();

    assertEquals(QuotaUtil.getReplicatedSize(KEY_SIZE, replicationConfig), totalUsedBytes);
    assertEquals(KEY_SIZE, totalUnreplicatedBytes);
  }
}
