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

package org.apache.hadoop.ozone.client.rpc.read;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.om.TestBucket;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests {@link StreamBlockInputStream}.
 */
public class TestStreamRead {
  {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("com"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ipc"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.server.http"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.container"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.ha"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.scm.safemode"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.hdds.utils"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.container.common"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.hadoop.ozone.om"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org.apache.ratis"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(CodecBuffer.class), Level.ERROR);
  }

  static final int CHUNK_SIZE = 1 << 20;          // 1MB
  static final int FLUSH_SIZE = 2 * CHUNK_SIZE;       // 2MB
  static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;   // 4MB

  static final int BLOCK_SIZE = 64 << 20;
  static final SizeInBytes KEY_SIZE = SizeInBytes.valueOf("128M");

  static MiniOzoneCluster newCluster(int bytesPerChecksum) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig config = conf.getObject(OzoneClientConfig.class);
    config.setBytesPerChecksum(bytesPerChecksum);
    conf.setFromObject(config);

    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 5);
    conf.setQuietMode(true);
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 64, StorageUnit.MB);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .build();
  }

  @Test
  void testReadKey512() throws Exception {
    final SizeInBytes bytesPerChecksum = SizeInBytes.valueOf(512);
    runTestReadKey(KEY_SIZE, bytesPerChecksum);
  }

  @Test
  void testReadKey16k() throws Exception {
    final SizeInBytes bytesPerChecksum = SizeInBytes.valueOf("16k");
    runTestReadKey(KEY_SIZE, bytesPerChecksum);
  }

  @Test
  void testReadKey256k() throws Exception {
    final SizeInBytes bytesPerChecksum = SizeInBytes.valueOf("256k");
    runTestReadKey(KEY_SIZE, bytesPerChecksum);
  }

  void runTestReadKey(SizeInBytes keySize, SizeInBytes bytesPerChecksum) throws Exception {
    try (MiniOzoneCluster cluster = newCluster(bytesPerChecksum.getSizeInt())) {
      cluster.waitForClusterToBeReady();

      System.out.println("cluster ready");

      OzoneConfiguration conf = cluster.getConf();
      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(true);
      OzoneConfiguration copy = new OzoneConfiguration(conf);
      copy.setFromObject(clientConfig);

      final int n = 5;
      final SizeInBytes writeBufferSize = SizeInBytes.valueOf("8MB");
      final SizeInBytes[] readBufferSizes = {
          SizeInBytes.valueOf("32M"),
          SizeInBytes.valueOf("8M"),
          SizeInBytes.valueOf("1M"),
          SizeInBytes.valueOf("4k"),
      };

      try (OzoneClient client = OzoneClientFactory.getRpcClient(copy)) {
        final TestBucket bucket = TestBucket.newBuilder(client).build();

        for (int i = 0; i < n; i++) {
          final String keyName = "key" + i;
          System.out.println("---------------------------------------------------------");
          System.out.printf("%s with %s bytes and %s bytesPerChecksum%n",
              keyName, keySize, bytesPerChecksum);

          final String md5 = createKey(bucket.delegate(), keyName, keySize, writeBufferSize);
          for (SizeInBytes readBufferSize : readBufferSizes) {
            runTestReadKey(keyName, keySize, readBufferSize, null, bucket);
            runTestReadKey(keyName, keySize, readBufferSize, md5, bucket);
          }
        }
      }
    }
  }

  static void print(String name, long keySizeByte, long elapsedNanos, SizeInBytes bufferSize, String computedMD5) {
    final double keySizeMb = keySizeByte * 1.0 / (1 << 20);
    final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    System.out.printf("%16s: %8.2f MB/s (%7.3f s, buffer %16s, keySize %8.2f MB, md5=%s)%n",
        name, keySizeMb / elapsedSeconds, elapsedSeconds, bufferSize, keySizeMb, computedMD5);
  }

  static String createKey(OzoneBucket bucket, String keyName, SizeInBytes keySize, SizeInBytes bufferSize)
      throws Exception {
    final byte[] buffer = new byte[bufferSize.getSizeInt()];
    ThreadLocalRandom.current().nextBytes(buffer);

    final long keySizeByte = keySize.getSize();
    final long startTime = System.nanoTime();
    try (OutputStream stream = bucket.createStreamKey(keyName, keySizeByte,
        RatisReplicationConfig.getInstance(ONE), Collections.emptyMap())) {
      for (long pos = 0; pos < keySizeByte;) {
        final int writeSize = Math.toIntExact(Math.min(buffer.length, keySizeByte - pos));
        stream.write(buffer, 0, writeSize);
        pos += writeSize;
      }
    }
    final long elapsedNanos = System.nanoTime() - startTime;

    final MessageDigest md5 = MessageDigest.getInstance("MD5");
    for (long pos = 0; pos < keySizeByte;) {
      final int writeSize = Math.toIntExact(Math.min(buffer.length, keySizeByte - pos));
      md5.update(buffer, 0, writeSize);
      pos += writeSize;
    }

    final String computedMD5 = StringUtils.bytes2Hex(md5.digest());
    print("createStreamKey", keySizeByte, elapsedNanos, bufferSize, computedMD5);
    return computedMD5;
  }

  private void runTestReadKey(String keyName, SizeInBytes keySize, SizeInBytes bufferSize, String expectedMD5,
      TestBucket bucket) throws Exception {
    final long keySizeByte = keySize.getSize();
    final MessageDigest md5 = MessageDigest.getInstance("MD5");
    // Read the data fully into a large enough byte array
    final byte[] buffer = new byte[bufferSize.getSizeInt()];
    final long startTime = System.nanoTime();
    try (KeyInputStream keyInputStream = bucket.getKeyInputStream(keyName)) {
      int pos = 0;
      for (; pos < keySizeByte;) {
        final int read = keyInputStream.read(buffer, 0, buffer.length);
        if (read == -1) {
          break;
        }

        if (expectedMD5 != null) {
          md5.update(buffer, 0, read);
        }
        pos += read;
      }
      assertEquals(keySizeByte, pos);
    }
    final long elapsedNanos = System.nanoTime() - startTime;

    final String computedMD5;
    if (expectedMD5 == null) {
      computedMD5 = null;
    } else {
      computedMD5 = StringUtils.bytes2Hex(md5.digest());
      assertEquals(expectedMD5, computedMD5);
    }
    print("readStreamKey", keySizeByte, elapsedNanos, bufferSize, computedMD5);
  }
}
