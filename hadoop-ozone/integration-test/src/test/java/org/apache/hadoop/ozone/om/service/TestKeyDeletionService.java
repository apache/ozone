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

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATA_DISTRIBUTION;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.HBASE_SUPPORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMPerformanceMetrics;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.upgrade.TestHddsUpgradeUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * DeletionService test to Pass Usage from OM to SCM.
 */
public class TestKeyDeletionService {
  private static final String CLIENT_ID = UUID.randomUUID().toString();
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final int KEY_SIZE = 5 * 1024; // 5 KB
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocol scmClient;
  private static OzoneBucket bucket = null;

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 500, TimeUnit.MILLISECONDS);
    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY, HBASE_SUPPORT.layoutVersion());

    InjectedUpgradeFinalizationExecutor<SCMUpgradeFinalizationContext> scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor<>();
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setUpgradeFinalizationExecutor(scmFinalizationExecutor);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(9)
        .setSCMConfigurator(configurator)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setLayoutVersion(HBASE_SUPPORT.layoutVersion()).build())
        .build();
    cluster.waitForClusterToBeReady();
    scmClient = cluster.getStorageContainerLocationClient();
    assertEquals(HBASE_SUPPORT.ordinal(), scmClient.getScmInfo().getMetaDataLayoutVersion());
    OzoneClient ozoneClient = cluster.newClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneClient.getObjectStore().createVolume(VOLUME_NAME);
    ozoneClient.getObjectStore().getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
    bucket = ozoneClient.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);
  }

  @AfterAll
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static Stream<Arguments> replicaType() {
    return Stream.of(
        arguments("RATIS", "ONE"),
        arguments("RATIS", "THREE")
    );
  }

  public static Stream<Arguments> ecType() {
    return Stream.of(
        arguments(ECReplicationConfig.EcCodec.RS, 3, 2, 2 * MB),
        arguments(ECReplicationConfig.EcCodec.RS, 6, 3, 2 * MB)
    );
  }

  @ParameterizedTest
  @MethodSource("replicaType")
  public void testDeletedKeyBytesPropagatedToSCM(String type, String factor) throws Exception {
    String keyName = UUID.randomUUID().toString();
    ReplicationConfig replicationConfig = RatisReplicationConfig
        .getInstance(ReplicationFactor.valueOf(factor).toProto());
    SCMPerformanceMetrics metrics = cluster.getStorageContainerManager().getBlockProtocolServer().getMetrics();
    long initialSuccessBlocks = metrics.getDeleteKeySuccessBlocks();
    long initialFailedBlocks = metrics.getDeleteKeyFailedBlocks();
    // Step 1: write a key
    createKey(keyName, replicationConfig);
    // Step 2: Spy on BlockManager and inject it into SCM
    BlockManager spyManager = injectSpyBlockManager(cluster);
    // Step 3: Delete the key (which triggers deleteBlocks call)
    bucket.deleteKey(keyName);
    // Step 4: Verify deleteBlocks call and capture argument
    verifyAndAssertQuota(spyManager, replicationConfig);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeySuccessBlocks() - initialSuccessBlocks == 1, 50, 1000);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeyFailedBlocks() - initialFailedBlocks == 0, 50, 1000);

    // Launch finalization from the client. In the current implementation,
    // this call will block until finalization completes.
    Future<?> finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(CLIENT_ID);
          } catch (IOException ex) {
            fail("finalization client failed", ex);
          }
        });
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    assertEquals(DATA_DISTRIBUTION.ordinal(), scmClient.getScmInfo().getMetaDataLayoutVersion());
    // create and delete another key to verify the process after feature is finalized
    keyName = UUID.randomUUID().toString();
    createKey(keyName, replicationConfig);
    bucket.deleteKey(keyName);
    verifyAndAssertQuota(spyManager, replicationConfig);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeySuccessBlocks() - initialSuccessBlocks == 2, 50, 1000);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeyFailedBlocks() - initialFailedBlocks == 0, 50, 1000);
  }

  @ParameterizedTest
  @MethodSource("ecType")
  void testGetDefaultShouldCreateECReplicationConfFromConfValues(
      ECReplicationConfig.EcCodec codec, int data, int parity, long chunkSize) throws Exception {
    String keyName = UUID.randomUUID().toString();
    ReplicationConfig replicationConfig = new ECReplicationConfig(data, parity, codec, (int) chunkSize);
    SCMPerformanceMetrics metrics = cluster.getStorageContainerManager().getBlockProtocolServer().getMetrics();
    long initialSuccessBlocks = metrics.getDeleteKeySuccessBlocks();
    long initialFailedBlocks = metrics.getDeleteKeyFailedBlocks();
    // Step 1: write a key
    createKey(keyName, replicationConfig);
    // Step 2: Spy on BlockManager and inject it into SCM
    BlockManager spyManager = injectSpyBlockManager(cluster);
    // Step 3: Delete the key (which triggers deleteBlocks call)
    bucket.deleteKey(keyName);
    // Step 4: Verify deleteBlocks call and capture argument
    verifyAndAssertQuota(spyManager, replicationConfig);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeySuccessBlocks() - initialSuccessBlocks == 1, 50, 1000);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeyFailedBlocks() - initialFailedBlocks == 0, 50, 1000);

    // Launch finalization from the client. In the current implementation,
    // this call will block until finalization completes.
    Future<?> finalizationFuture = Executors.newSingleThreadExecutor().submit(
        () -> {
          try {
            scmClient.finalizeScmUpgrade(CLIENT_ID);
          } catch (IOException ex) {
            fail("finalization client failed", ex);
          }
        });
    finalizationFuture.get();
    TestHddsUpgradeUtils.waitForFinalizationFromClient(scmClient, CLIENT_ID);
    assertEquals(DATA_DISTRIBUTION.ordinal(), scmClient.getScmInfo().getMetaDataLayoutVersion());
    // create and delete another key to verify the process after feature is finalized
    keyName = UUID.randomUUID().toString();
    createKey(keyName, replicationConfig);
    bucket.deleteKey(keyName);
    verifyAndAssertQuota(spyManager, replicationConfig);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeySuccessBlocks() - initialSuccessBlocks == 2, 50, 1000);
    GenericTestUtils.waitFor(() -> metrics.getDeleteKeyFailedBlocks() - initialFailedBlocks == 0, 50, 1000);
  }

  private void createKey(String keyName, ReplicationConfig replicationConfig) throws IOException {
    byte[] data = new byte[KEY_SIZE];
    try (OzoneOutputStream out = bucket.createKey(keyName, KEY_SIZE,
        replicationConfig, new HashMap<>())) {
      out.write(data);
    }
  }

  private BlockManager injectSpyBlockManager(MiniOzoneCluster miniOzoneCluster) throws Exception {
    StorageContainerManager scm = miniOzoneCluster.getStorageContainerManager();
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

    if (captor.getAllValues().stream().anyMatch(blockGroups -> blockGroups.stream().anyMatch(
        group -> group.getAllDeletedBlocks().isEmpty()))) {
      assertEquals(1, captor.getAllValues().get(0).get(0).getBlockIDs().size());
      assertEquals(0, captor.getAllValues().get(0).get(0).getAllDeletedBlocks().size());
      return;
    }

    long totalUsedBytes = captor.getAllValues().get(0).stream()
        .flatMap(group -> group.getAllDeletedBlocks().stream())
        .mapToLong(DeletedBlock::getReplicatedSize).sum();

    long totalUnreplicatedBytes = captor.getAllValues().get(0).stream()
        .flatMap(group -> group.getAllDeletedBlocks().stream())
        .mapToLong(DeletedBlock::getSize).sum();

    assertEquals(0, captor.getAllValues().get(0).get(0).getBlockIDs().size());
    assertEquals(1, captor.getAllValues().get(0).get(0).getAllDeletedBlocks().size());
    assertEquals(QuotaUtil.getReplicatedSize(KEY_SIZE, replicationConfig), totalUsedBytes);
    assertEquals(KEY_SIZE, totalUnreplicatedBytes);
  }
}
