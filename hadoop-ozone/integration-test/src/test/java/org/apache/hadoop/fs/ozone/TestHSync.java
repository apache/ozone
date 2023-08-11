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
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.crypto.Encryptor;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test HSync.
 */
@Timeout(value = 300)
public class TestHSync {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHSync.class);

  private static MiniOzoneCluster cluster;
  private static OzoneBucket bucket;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    CONF.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    CONF.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
    CONF.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    cluster = MiniOzoneCluster.newBuilder(CONF)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(10)
        .setBlockSize(blockSize)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(flushSize)
        .setStreamBufferMaxSize(maxFlushSize)
        .setDataStreamBufferFlushize(maxFlushSize)
        .setStreamBufferSizeUnit(StorageUnit.BYTES)
        .setDataStreamMinPacketSize(chunkSize)
        .setDataStreamStreamWindowSize(5 * chunkSize)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);

    // Enable DEBUG level logging for relevant classes
    GenericTestUtils.setLogLevel(OMKeyRequest.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequest.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(OMKeyCommitRequestWithFSO.LOG, Level.DEBUG);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
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
          Assertions.fail("deletedTable should not have such entry. key = " +
              key);
        }
      }
    }
  }

  @Test
  public void testO3fsHSync() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    try (FileSystem fs = FileSystem.get(CONF)) {
      for (int i = 0; i < 10; i++) {
        final Path file = new Path("/file" + i);
        runTestHSync(fs, file, 1 << i);
      }
    }
  }

  @Test
  public void testOfsHSync() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, CONF.get(OZONE_OM_ADDRESS_KEY));
    CONF.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();

    try (FileSystem fs = FileSystem.get(CONF)) {
      for (int i = 0; i < 10; i++) {
        final Path file = new Path(dir, "file" + i);
        runTestHSync(fs, file, 1 << i);
      }
    }
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
      Assertions.assertEquals(length, skipped);

      for (; ;) {
        final int n = in.read(buffer, 0, buffer.length);
        if (n <= 0) {
          break;
        }
        for (int i = 0; i < n; i++) {
          assertEquals(data[offset + i], buffer[i]);
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
      for (int i = 0; i < 10; i++) {
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
}
