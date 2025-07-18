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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_LEASE_HARD_LIMIT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isDone;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.isStarting;
import static org.apache.ozone.test.LambdaTestUtils.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.metadata.AbstractDatanodeStore;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.service.OpenKeyCleanupService;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.event.Level;

/**
 * Test HSync upgrade.
 */
@TestMethodOrder(OrderAnnotation.class)
public class TestHSyncUpgrade {
  private MiniOzoneCluster cluster;
  private OzoneBucket bucket;

  private final OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.FILE_SYSTEM_OPTIMIZED;

  private static final int CHUNK_SIZE = 4 << 12;
  private static final int FLUSH_SIZE = 3 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;
  private static final int SERVICE_INTERVAL = 100;
  private static final int EXPIRE_THRESHOLD_MS = 140;

  private static final int POLL_INTERVAL_MILLIS = 500;
  private static final int POLL_MAX_WAIT_MILLIS = 120_000;

  @BeforeEach
  public void init() throws Exception {
    final BucketLayout layout = BUCKET_LAYOUT;

    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // Reduce KeyDeletingService interval
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setBoolean("ozone.client.incremental.chunk.list", true);
    conf.setBoolean("ozone.client.stream.putblock.piggybacking", true);
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        SERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_LEASE_HARD_LIMIT,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    conf.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.MULTITENANCY_SCHEMA.layoutVersion());

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .setDataStreamBufferFlushSize(MAX_FLUSH_SIZE)
        .setDataStreamMinPacketSize(CHUNK_SIZE)
        .setDataStreamWindowSize(5 * CHUNK_SIZE)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);

    // Enable DEBUG level logging for relevant classes
    GenericTestUtils.setLogLevel(BlockManagerImpl.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(AbstractDatanodeStore.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockOutputStream.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockInputStream.class, Level.DEBUG);
    GenericTestUtils.setLogLevel(KeyValueHandler.class, Level.DEBUG);

    GenericTestUtils.setLogLevel(BufferPool.class, Level.DEBUG);

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) cluster.getOzoneManager().getKeyManager()
            .getOpenKeyCleanupService();
    openKeyCleanupService.suspend();
  }

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void upgrade() throws Exception {
    preFinalizationChecks();
    finalizeOMUpgrade();
  }

  private void preFinalizationChecks() throws IOException {
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    final Path file = new Path(dir, "pre-finalization");
    try (RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf)) {
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        OMException omException  = assertThrows(OMException.class, outputStream::hsync);
        assertFinalizationExceptionForHsync(omException);
      }
      final OzoneManagerProtocol omClient = client.getObjectStore()
          .getClientProxy().getOzoneManagerClient();
      OMException omException  = assertThrows(OMException.class,
          () -> omClient.listOpenFiles("", 100, ""));
      assertFinalizationException(omException);

      omException = assertThrows(OMException.class,
          () -> fs.recoverLease(file));
      assertFinalizationException(omException);

      fs.delete(file, false);
    }
  }

  private void assertFinalizationExceptionForHsync(OMException omException) {
    assertEquals(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omException.getResult());
    assertThat(omException.getMessage())
        .contains("Cluster does not have the hsync support feature finalized yet");
  }

  private void assertFinalizationException(OMException omException) {
    assertEquals(NOT_SUPPORTED_OPERATION_PRIOR_FINALIZATION,
        omException.getResult());
    assertThat(omException.getMessage())
        .contains("cannot be invoked before finalization.");
  }

  /**
   * Trigger OM upgrade finalization from the client and block until completion
   * (status FINALIZATION_DONE).
   */
  private void finalizeOMUpgrade() throws Exception {
    // Trigger OM upgrade finalization. Ref: FinalizeUpgradeSubCommand#call
    final OzoneManagerProtocol omClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    final String upgradeClientID = "Test-Upgrade-Client-" + UUID.randomUUID();
    UpgradeFinalization.StatusAndMessages finalizationResponse =
        omClient.finalizeUpgrade(upgradeClientID);

    // The status should transition as soon as the client call above returns
    assertTrue(isStarting(finalizationResponse.status()));
    // Wait for the finalization to be marked as done.
    // 10s timeout should be plenty.
    await(POLL_MAX_WAIT_MILLIS, POLL_INTERVAL_MILLIS, () -> {
      final UpgradeFinalization.StatusAndMessages progress =
          omClient.queryUpgradeFinalizationProgress(
              upgradeClientID, false, false);
      return isDone(progress.status());
    });
  }

}
