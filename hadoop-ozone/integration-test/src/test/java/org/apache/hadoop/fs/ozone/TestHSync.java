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

package org.apache.hadoop.fs.ozone;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;

import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.impl.BlockManagerImpl;
import org.apache.hadoop.ozone.container.metadata.AbstractDatanodeStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.service.OpenKeyCleanupService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_CHUNK_LIST_INCREMENTAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_LEASE_HARD_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test HSync.
 */
@Timeout(value = 300)
@TestMethodOrder(OrderAnnotation.class)
public class TestHSync {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHSync.class);

  private static MiniOzoneCluster cluster;
  private static OzoneBucket bucket;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static OzoneClient client;
  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.FILE_SYSTEM_OPTIMIZED;

  private static final int CHUNK_SIZE = 4 << 12;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;
  private static final int SERVICE_INTERVAL = 100;
  private static final int EXPIRE_THRESHOLD_MS = 140;

  private static OpenKeyCleanupService openKeyCleanupService;

  @BeforeAll
  public static void init() throws Exception {
    final BucketLayout layout = BUCKET_LAYOUT;

    CONF.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    CONF.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    CONF.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    CONF.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // Reduce KeyDeletingService interval
    CONF.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    CONF.setBoolean("ozone.client.incremental.chunk.list", true);
    CONF.setBoolean("ozone.client.stream.putblock.piggybacking", true);
    CONF.setBoolean(OZONE_CHUNK_LIST_INCREMENTAL, true);
    CONF.setTimeDuration(OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        SERVICE_INTERVAL, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    CONF.setTimeDuration(OZONE_OM_LEASE_HARD_LIMIT,
        EXPIRE_THRESHOLD_MS, TimeUnit.MILLISECONDS);
    CONF.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .setDataStreamBufferFlushSize(MAX_FLUSH_SIZE)
        .setDataStreamMinPacketSize(CHUNK_SIZE)
        .setDataStreamWindowSize(5 * CHUNK_SIZE)
        .applyTo(CONF);

    cluster = MiniOzoneCluster.newBuilder(CONF)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);

    // Enable DEBUG level logging for relevant classes
    GenericTestUtils.setLogLevel(BlockManagerImpl.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(AbstractDatanodeStore.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockOutputStream.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(BlockInputStream.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(KeyValueHandler.LOG, Level.DEBUG);

    openKeyCleanupService =
        (OpenKeyCleanupService) cluster.getOzoneManager().getKeyManager().getOpenKeyCleanupService();
    openKeyCleanupService.suspend();
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  // Making this the first test to be run to avoid db key composition headaches
  @Order(1)
  public void testKeyMetadata() throws Exception {
    // Tests key metadata behavior upon create(), hsync() and close():
    // 1. When a key is create()'d, neither OpenKeyTable nor KeyTable entry shall have hsync metadata.
    // 2. When the key is hsync()'ed, both OpenKeyTable and KeyTable shall have hsync metadata.
    // 3. When the key is hsync()'ed again, both OpenKeyTable and KeyTable shall have hsync metadata.
    // 4. When the key is close()'d, KeyTable entry shall not have hsync metadata. Key shall not exist in OpenKeyTable.

    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final String keyName = "file-test-key-metadata";
    final Path file = new Path(dir, keyName);

    OMMetadataManager omMetadataManager =
        cluster.getOzoneManager().getMetadataManager();

    // Expect empty OpenKeyTable and KeyTable before key creation
    Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(BUCKET_LAYOUT);
    assertTrue(openKeyTable.isEmpty());
    Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable(BUCKET_LAYOUT);
    assertTrue(keyTable.isEmpty());

    try (FileSystem fs = FileSystem.get(CONF)) {
      try (FSDataOutputStream os = fs.create(file, true)) {
        // Wait for double buffer flush to avoid flakiness because RDB iterator bypasses table cache
        cluster.getOzoneManager().awaitDoubleBufferFlush();
        // OpenKeyTable key should NOT have HSYNC_CLIENT_ID
        OmKeyInfo keyInfo = getFirstKeyInTable(keyName, openKeyTable);
        assertFalse(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));
        // KeyTable should still be empty
        assertTrue(keyTable.isEmpty());

        os.hsync();
        cluster.getOzoneManager().awaitDoubleBufferFlush();
        // OpenKeyTable key should have HSYNC_CLIENT_ID now
        keyInfo = getFirstKeyInTable(keyName, openKeyTable);
        assertTrue(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));
        // KeyTable key should be there and have HSYNC_CLIENT_ID
        keyInfo = getFirstKeyInTable(keyName, keyTable);
        assertTrue(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));

        // hsync again, metadata should not change
        os.hsync();
        cluster.getOzoneManager().awaitDoubleBufferFlush();
        keyInfo = getFirstKeyInTable(keyName, openKeyTable);
        assertTrue(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));
        keyInfo = getFirstKeyInTable(keyName, keyTable);
        assertTrue(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));
      }
      // key is closed, OpenKeyTable should be empty
      cluster.getOzoneManager().awaitDoubleBufferFlush();
      assertTrue(openKeyTable.isEmpty());
      // KeyTable should have the key. But the key shouldn't have metadata HSYNC_CLIENT_ID anymore
      OmKeyInfo keyInfo = getFirstKeyInTable(keyName, keyTable);
      assertFalse(keyInfo.getMetadata().containsKey(OzoneConsts.HSYNC_CLIENT_ID));

      // Clean up
      assertTrue(fs.delete(file, false));
      // Wait for KeyDeletingService to finish to avoid interfering other tests
      Table<String, RepeatedOmKeyInfo> deletedTable = omMetadataManager.getDeletedTable();
      GenericTestUtils.waitFor(
          () -> {
            try {
              return deletedTable.isEmpty();
            } catch (IOException e) {
              return false;
            }
          }, 250, 10000);
    }
  }

  @Test
  public void testKeyHSyncThenClose() throws Exception {
    // Check that deletedTable should not have keys with the same block as in
    // keyTable's when a key is hsync()'ed then close()'d.

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    String data = "random data";
    final Path file = new Path(dir, "file-hsync-then-close");
    try (FileSystem fs = FileSystem.get(CONF)) {
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        outputStream.write(data.getBytes(UTF_8), 0, data.length());
        outputStream.hsync();
      }
    }

    OzoneManager ozoneManager = cluster.getOzoneManager();
    // Wait for double buffer to trigger all pending addToDBBatch(),
    // including OMKeyCommitResponse(WithFSO)'s that writes to deletedTable.
    ozoneManager.awaitDoubleBufferFlush();

    OMMetadataManager metadataManager = ozoneManager.getMetadataManager();
    // deletedTable should not have an entry for file at all in this case
    try (TableIterator<String,
        ? extends Table.KeyValue<String, RepeatedOmKeyInfo>>
        tableIter = metadataManager.getDeletedTable().iterator()) {
      while (tableIter.hasNext()) {
        Table.KeyValue<String, RepeatedOmKeyInfo> kv = tableIter.next();
        String key = kv.getKey();
        if (key.startsWith(file.toString())) {
          RepeatedOmKeyInfo val = kv.getValue();
          LOG.error("Unexpected deletedTable entry: key = {}, val = {}",
              key, val);
          fail("deletedTable should not have such entry. key = " + key);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testO3fsHSync(boolean incrementalChunkList) throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    initClientConfig(incrementalChunkList);
    try (FileSystem fs = FileSystem.get(CONF)) {
      for (int i = 0; i < 10; i++) {
        final Path file = new Path("/file" + i);
        runTestHSync(fs, file, 1 << i);
      }
    }
  }


  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testOfsHSync(boolean incrementalChunkList) throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    initClientConfig(incrementalChunkList);
    try (FileSystem fs = FileSystem.get(CONF)) {
      for (int i = 0; i < 10; i++) {
        final Path file = new Path(dir, "file" + i);
        runTestHSync(fs, file, 1 << i);
      }
    }
  }

  @Test
  public void testHSyncDeletedKey() throws Exception {
    // Verify that a key can't be successfully hsync'ed again after it's deleted,
    // and that key won't reappear after a failed hsync.

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final Path key1 = new Path(dir, "key-hsync-del");

    try (FileSystem fs = FileSystem.get(CONF)) {
      // Create key1
      try (FSDataOutputStream os = fs.create(key1, true)) {
        os.write(1);
        os.hsync();
        fs.delete(key1, false);

        // getFileStatus should throw FNFE because the key is deleted
        assertThrows(FileNotFoundException.class,
            () -> fs.getFileStatus(key1));
        // hsync should throw because the open key is gone
        try {
          os.hsync();
        } catch (OMException omEx) {
          assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, omEx.getResult());
        }
        // key1 should not reappear after failed hsync
        assertThrows(FileNotFoundException.class,
            () -> fs.getFileStatus(key1));
      } catch (OMException ex) {
        // os.close() throws OMException because the key is deleted
        assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, ex.getResult());
      }
    }
  }

  @Test
  public void testHSyncOpenKeyDeletionWhileDeleteDirectory() throws Exception {
    // Verify that when directory is deleted recursively hsync related openKeys should be deleted,

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName() + OZONE_URI_DELIMITER + "dir1/dir2";
    final Path key1 = new Path(dir, "hsync-key");

    try (FileSystem fs = FileSystem.get(CONF)) {
      // Create key1
      try (FSDataOutputStream os = fs.create(key1, true)) {
        os.write(1);
        os.hsync();
        // There should be 1 key in openFileTable
        assertThat(1 == getOpenKeyInfo(BucketLayout.FILE_SYSTEM_OPTIMIZED).size());
        // Delete directory recursively
        fs.delete(new Path(OZONE_ROOT + bucket.getVolumeName() + OZONE_URI_DELIMITER +
            bucket.getName() + OZONE_URI_DELIMITER + "dir1/"), true);

        // Verify if DELETED_HSYNC_KEY metadata is added to openKey
        GenericTestUtils.waitFor(() -> {
          List<OmKeyInfo> omKeyInfo = getOpenKeyInfo(BucketLayout.FILE_SYSTEM_OPTIMIZED);
          return omKeyInfo.size() > 0 && omKeyInfo.get(0).getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY);
        }, 1000, 12000);

        // Resume openKeyCleanupService
        openKeyCleanupService.resume();

        // Verify entry from openKey gets deleted eventually
        GenericTestUtils.waitFor(() ->
            0 == getOpenKeyInfo(BucketLayout.FILE_SYSTEM_OPTIMIZED).size(), 1000, 12000);
      } catch (OMException ex) {
        assertEquals(OMException.ResultCodes.DIRECTORY_NOT_FOUND, ex.getResult());
      } finally {
        openKeyCleanupService.suspend();
      }
    }
  }

  private List<OmKeyInfo> getOpenKeyInfo(BucketLayout bucketLayout) {
    List<OmKeyInfo> omKeyInfo = new ArrayList<>();

    Table<String, OmKeyInfo> openFileTable =
        cluster.getOzoneManager().getMetadataManager().getOpenKeyTable(bucketLayout);
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             iterator = openFileTable.iterator()) {
      while (iterator.hasNext()) {
        omKeyInfo.add(iterator.next().getValue());
      }
    } catch (Exception e) {
    }
    return omKeyInfo;
  }

  @Test
  public void testUncommittedBlocks() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    final byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(data);

    try (FileSystem fs = FileSystem.get(CONF)) {
      final Path file = new Path(dir, "file");
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        outputStream.hsync();
        outputStream.write(data);
        outputStream.hsync();
        assertTrue(cluster.getOzoneManager().getMetadataManager()
            .getDeletedTable().isEmpty());
      }
    }
  }

  @Test
  public void testOverwriteHSyncFile() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    try (FileSystem fs = FileSystem.get(CONF)) {
      final Path file = new Path(dir, "fileoverwrite");
      try (FSDataOutputStream os = fs.create(file, false)) {
        os.hsync();
        UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
            "user2", new String[] {"group1"});
        assertThrows(FileAlreadyExistsException.class,
            () -> ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
              try (FSDataOutputStream os1 = fs.create(file, false)) {
                os1.hsync();
              }
              return null;
            }));
        os.hsync();
      }
    }
  }

  @Test
  public void testHsyncKeyCallCount() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    OMMetrics omMetrics = cluster.getOzoneManager().getMetrics();
    omMetrics.resetNumKeyHSyncs();
    final byte[] data = new byte[128];
    ThreadLocalRandom.current().nextBytes(data);

    final Path file = new Path(dir, "file-hsync-then-close");
    try (FileSystem fs = FileSystem.get(CONF)) {
      long fileSize = 0;
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        // make sure at least writing 2 blocks data
        while (fileSize <= BLOCK_SIZE) {
          outputStream.write(data, 0, data.length);
          outputStream.hsync();
          fileSize += data.length;
        }
      }
    }
    assertEquals(2, omMetrics.getNumKeyHSyncs());

    // test file with all blocks pre-allocated
    omMetrics.resetNumKeyHSyncs();
    long writtenSize = 0;
    try (OzoneOutputStream outputStream = bucket.createKey("key-" + RandomStringUtils.randomNumeric(5),
        BLOCK_SIZE * 2, ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>())) {
      // make sure at least writing 2 blocks data
      while (writtenSize <= BLOCK_SIZE) {
        outputStream.write(data, 0, data.length);
        outputStream.hsync();
        writtenSize += data.length;
      }
    }
    assertEquals(2, omMetrics.getNumKeyHSyncs());
  }

  static void runTestHSync(FileSystem fs, Path file, int initialDataSize)
      throws Exception {
    try (StreamWithLength out = new StreamWithLength(
        fs.create(file, true))) {
      runTestHSync(fs, file, out, initialDataSize);
      for (int i = 1; i < 5; i++) {
        for (int j = -1; j <= 1; j++) {
          int dataSize = (1 << (i * 5)) + j;
          runTestHSync(fs, file, out, dataSize);
        }
      }
    }
  }

  private static class StreamWithLength implements Closeable {
    private final FSDataOutputStream out;
    private long length = 0;

    StreamWithLength(FSDataOutputStream out) {
      this.out = out;
    }

    long getLength() {
      return length;
    }

    void writeAndHsync(byte[] data) throws IOException {
      out.write(data);
      out.hsync();
      length += data.length;
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
  }

  static void runTestHSync(FileSystem fs, Path file,
      StreamWithLength out, int dataSize)
      throws Exception {
    final long length = out.getLength();
    LOG.info("runTestHSync {} with size {}, skipLength={}",
        file, dataSize, length);
    final byte[] data = new byte[dataSize];
    ThreadLocalRandom.current().nextBytes(data);
    out.writeAndHsync(data);

    final byte[] buffer = new byte[4 << 10];
    int offset = 0;
    try (FSDataInputStream in = fs.open(file)) {
      final long skipped = in.skip(length);
      assertEquals(length, skipped);

      for (; ;) {
        final int n = in.read(buffer, 0, buffer.length);
        if (n <= 0) {
          break;
        }
        for (int i = 0; i < n; i++) {
          assertEquals(data[offset + i], buffer[i],
              "expected at offset " + offset + " i=" + i);
        }
        offset += n;
      }
    }
    assertEquals(data.length, offset);
  }

  private void runConcurrentWriteHSync(Path file,
      final FSDataOutputStream out, int initialDataSize)
      throws InterruptedException, IOException {
    final byte[] data = new byte[initialDataSize];
    ThreadLocalRandom.current().nextBytes(data);

    AtomicReference<IOException> writerException = new AtomicReference<>();
    AtomicReference<IOException> syncerException = new AtomicReference<>();

    LOG.info("runConcurrentWriteHSync {} with size {}",
        file, initialDataSize);

    final long start = Time.monotonicNow();
    // two threads: write and hsync
    Runnable writer = () -> {
      while ((Time.monotonicNow() - start < 10000)) {
        try {
          out.write(data);
        } catch (IOException e) {
          writerException.set(e);
          throw new RuntimeException(e);
        }
      }
    };

    Runnable syncer = () -> {
      while ((Time.monotonicNow() - start < 10000)) {
        try {
          out.hsync();
        } catch (IOException e) {
          syncerException.set(e);
          throw new RuntimeException(e);
        }
      }
    };

    Thread writerThread = new Thread(writer);
    writerThread.start();
    Thread syncThread = new Thread(syncer);
    syncThread.start();
    writerThread.join();
    syncThread.join();

    if (writerException.get() != null) {
      throw writerException.get();
    }
    if (syncerException.get() != null) {
      throw syncerException.get();
    }
  }

  @Test
  public void testConcurrentWriteHSync()
      throws IOException, InterruptedException {
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    try (FileSystem fs = FileSystem.get(CONF)) {
      for (int i = 0; i < 5; i++) {
        final Path file = new Path(dir, "file" + i);
        try (FSDataOutputStream out =
            fs.create(file, true)) {
          int initialDataSize = 1 << i;
          runConcurrentWriteHSync(file, out, initialDataSize);
        }

        fs.delete(file, false);
      }
    }
  }

  @Test
  public void testStreamCapability() throws Exception {
    final String rootPath = String.format("%s://%s/",
            OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
            + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, "file");

    try (FileSystem fs = FileSystem.get(CONF);
         FSDataOutputStream os = fs.create(file, true)) {
      // Verify output stream supports hsync() and hflush().
      assertTrue(os.hasCapability(StreamCapabilities.HFLUSH),
          "KeyOutputStream should support hflush()!");
      assertTrue(os.hasCapability(StreamCapabilities.HSYNC),
          "KeyOutputStream should support hsync()!");
    }

    testEncryptedStreamCapabilities(false);
  }

  @Test
  public void testECStreamCapability() throws Exception {
    // create EC bucket to be used by OzoneFileSystem
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    builder.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig(
                3, 2, ECReplicationConfig.EcCodec.RS, (int) OzoneConsts.MB)));
    BucketArgs omBucketArgs = builder.build();
    String ecBucket = UUID.randomUUID().toString();
    TestDataUtil.createBucket(client, bucket.getVolumeName(), omBucketArgs,
        ecBucket);
    String ecUri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, ecBucket, bucket.getVolumeName());
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, ecUri);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, "file");

    try (FileSystem fs = FileSystem.get(CONF);
         FSDataOutputStream os = fs.create(file, true)) {
      // Verify output stream supports hsync() and hflush().
      assertFalse(os.hasCapability(StreamCapabilities.HFLUSH),
          "ECKeyOutputStream should not support hflush()!");
      assertFalse(os.hasCapability(StreamCapabilities.HSYNC),
          "ECKeyOutputStream should not support hsync()!");
    }
    testEncryptedStreamCapabilities(true);
  }

  @Test
  public void testDisableHsync() throws Exception {
    // When hsync is disabled, client does not throw exception.
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    CONF.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, false);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    final byte[] data = new byte[1];
    ThreadLocalRandom.current().nextBytes(data);

    try (FileSystem fs = FileSystem.get(CONF)) {
      final Path file = new Path(dir, "file_hsync_disable");
      try (FSDataOutputStream outputStream = fs.create(file, true)) {
        outputStream.hsync();
        assertThrows(FileNotFoundException.class,
            () -> fs.getFileStatus(file));
      }
    } finally {
      // re-enable the feature flag
      CONF.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    }
  }

  /**
   * Helper method to check and get the first key in the OpenKeyTable.
   * @param keyName expect key name to contain this string
   * @param openKeyTable Table<String, OmKeyInfo>
   * @return OmKeyInfo
   */
  private OmKeyInfo getFirstKeyInTable(String keyName, Table<String, OmKeyInfo> openKeyTable) throws IOException {
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>> it = openKeyTable.iterator()) {
      assertTrue(it.hasNext());
      Table.KeyValue<String, OmKeyInfo> kv = it.next();
      String dbOpenKey = kv.getKey();
      assertNotNull(dbOpenKey);
      assertTrue(dbOpenKey.contains(keyName));
      return kv.getValue();
    }
  }

  private void testEncryptedStreamCapabilities(boolean isEC) throws IOException,
      GeneralSecurityException {
    KeyOutputStream kos;
    if (isEC) {
      kos = mock(ECKeyOutputStream.class);
    } else {
      kos = mock(KeyOutputStream.class);
    }
    CryptoCodec codec = mock(CryptoCodec.class);
    when(codec.getCipherSuite()).thenReturn(CipherSuite.AES_CTR_NOPADDING);
    when(codec.getConf()).thenReturn(CONF);
    Encryptor encryptor = mock(Encryptor.class);
    when(codec.createEncryptor()).thenReturn(encryptor);
    CryptoOutputStream cos =
        new CryptoOutputStream(kos, codec, new byte[0], new byte[0]);
    OzoneOutputStream oos = new OzoneOutputStream(cos, true);
    OzoneFSOutputStream ofso = new OzoneFSOutputStream(oos);

    try (CapableOzoneFSOutputStream cofsos =
        new CapableOzoneFSOutputStream(ofso, true)) {
      if (isEC) {
        assertFalse(cofsos.hasCapability(StreamCapabilities.HFLUSH));
      } else {
        assertTrue(cofsos.hasCapability(StreamCapabilities.HFLUSH));
      }
    }
    try (CapableOzoneFSOutputStream cofsos =
        new CapableOzoneFSOutputStream(ofso, false)) {
      assertFalse(cofsos.hasCapability(StreamCapabilities.HFLUSH));
    }
  }

  public void initClientConfig(boolean incrementalChunkList) {
    OzoneClientConfig clientConfig = CONF.getObject(OzoneClientConfig.class);
    clientConfig.setIncrementalChunkList(incrementalChunkList);
    clientConfig.setChecksumType(ContainerProtos.ChecksumType.CRC32C);
    CONF.setFromObject(clientConfig);
  }

  public static Stream<Arguments> parameters1() {
    return Stream.of(
        arguments(true, 512),
        arguments(true, 511),
        arguments(true, 513),
        arguments(false, 512),
        arguments(false, 511),
        arguments(false, 513)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters1")
  public void writeWithSmallBuffer(boolean incrementalChunkList, int bufferSize)
      throws IOException {
    initClientConfig(incrementalChunkList);

    final String keyName = UUID.randomUUID().toString();
    int fileSize = 16 << 11;
    String s = RandomStringUtils.randomAlphabetic(bufferSize);
    ByteBuffer byteBuffer = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));

    int writtenSize = 0;
    try (OzoneOutputStream out = bucket.createKey(keyName, fileSize,
        ReplicationConfig.getDefault(CONF), new HashMap<>())) {
      while (writtenSize < fileSize) {
        int len = Math.min(bufferSize, fileSize - writtenSize);
        out.write(byteBuffer, 0, len);
        out.hsync();
        writtenSize += bufferSize;
      }
    }

    OzoneKeyDetails keyInfo = bucket.getKey(keyName);
    assertEquals(fileSize, keyInfo.getDataSize());

    int readSize = 0;
    try (OzoneInputStream is = bucket.readKey(keyName)) {
      while (readSize < fileSize) {
        int len = Math.min(bufferSize, fileSize - readSize);
        ByteBuffer readBuffer = ByteBuffer.allocate(len);
        int readLen = is.read(readBuffer);
        assertEquals(len, readLen);
        if (len < bufferSize) {
          for (int i = 0; i < len; i++) {
            assertEquals(readBuffer.array()[i], byteBuffer.array()[i]);
          }
        } else {
          assertArrayEquals(readBuffer.array(), byteBuffer.array());
        }
        readSize += readLen;
      }
    }
    bucket.deleteKey(keyName);
  }

  public static Stream<Arguments> parameters2() {
    return Stream.of(
        arguments(true, 1024 * 1024 + 1),
        arguments(true, 1024 * 1024 + 1 + CHUNK_SIZE),
        arguments(true, 1024 * 1024 - 1 + CHUNK_SIZE),
        arguments(false, 1024 * 1024 + 1),
        arguments(false, 1024 * 1024 + 1 + CHUNK_SIZE),
        arguments(false, 1024 * 1024 - 1 + CHUNK_SIZE)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters2")
  public void writeWithBigBuffer(boolean incrementalChunkList, int bufferSize)
      throws IOException {
    initClientConfig(incrementalChunkList);

    final String keyName = UUID.randomUUID().toString();
    int count = 2;
    int fileSize = bufferSize * count;
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);

    try (OzoneOutputStream out = bucket.createKey(keyName, fileSize,
        ReplicationConfig.getDefault(CONF), new HashMap<>())) {
      for (int i = 0; i < count; i++) {
        out.write(byteBuffer);
        out.hsync();
      }
    }

    OzoneKeyDetails keyInfo = bucket.getKey(keyName);
    assertEquals(fileSize, keyInfo.getDataSize());
    int totalReadLen = 0;
    try (OzoneInputStream is = bucket.readKey(keyName)) {

      for (int i = 0; i < count; i++) {
        ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
        int readLen = is.read(readBuffer);
        if (bufferSize != readLen) {
          throw new IOException("failed to read " + bufferSize + " from offset " + totalReadLen +
              ", actually read " + readLen + ", block " + totalReadLen /
              BLOCK_SIZE);
        }
        assertArrayEquals(byteBuffer.array(), readBuffer.array());
        totalReadLen += readLen;
      }
    }
    bucket.deleteKey(keyName);
  }
}
