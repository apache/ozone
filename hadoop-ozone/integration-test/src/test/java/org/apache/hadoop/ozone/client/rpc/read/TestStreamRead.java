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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.om.TestBucket;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests {@link StreamBlockInputStream}.
 */
public class TestStreamRead {
  {
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("com"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("org"), Level.ERROR);

    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("BackgroundPipelineScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("ExpiredContainerReplicaOpScrubber"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger("SCMHATransactionMonitor"), Level.ERROR);
    GenericTestUtils.setLogLevel(LoggerFactory.getLogger(CodecBuffer.class), Level.ERROR);
  }

  static final int CHUNK_SIZE = 1 << 20;          // 1MB
  static final int FLUSH_SIZE = 2 * CHUNK_SIZE;       // 2MB
  static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;   // 4MB

  static final SizeInBytes KEY_SIZE = SizeInBytes.valueOf("128M");
  static final int BLOCK_SIZE = KEY_SIZE.getSizeInt();

  static final String DUMMY_KEY = "dummyKey";

  static MiniOzoneCluster newCluster(int bytesPerChecksum) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig config = conf.getObject(OzoneClientConfig.class);
    config.setBytesPerChecksum(bytesPerChecksum);
    conf.setFromObject(config);

    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setInt(ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT, 1);
    conf.setQuietMode(true);

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
    System.out.println("cluster starting ...");
    try (MiniOzoneCluster cluster = newCluster(bytesPerChecksum.getSizeInt())) {
      cluster.waitForClusterToBeReady();
      System.out.println("cluster ready");

      final List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
      assertEquals(1, datanodes.size());
      final HddsDatanodeService datanode = datanodes.get(0);

      OzoneConfiguration conf = cluster.getConf();
      OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
      clientConfig.setStreamReadBlock(true);
      final OzoneConfiguration steamReadConf = new OzoneConfiguration(conf);
      steamReadConf.setFromObject(clientConfig);

      clientConfig.setStreamReadBlock(false);
      final OzoneConfiguration nonSteamReadConf = new OzoneConfiguration(conf);
      nonSteamReadConf.setFromObject(clientConfig);

      final SizeInBytes[] bufferSizes = {
          SizeInBytes.valueOf("32M"),
          SizeInBytes.valueOf("8M"),
          SizeInBytes.valueOf("1M"),
          SizeInBytes.valueOf("4k"),
      };

      try (OzoneClient streamReadClient = OzoneClientFactory.getRpcClient(steamReadConf);
           OzoneClient nonStreamReadClient = OzoneClientFactory.getRpcClient(nonSteamReadConf)) {
        final TestBucket testBucket = TestBucket.newBuilder(streamReadClient).build();
        final String volume = testBucket.delegate().getVolumeName();
        final String bucket = testBucket.delegate().getName();
        final String keyName = "key0";

        // get the client ready by writing a dummy key
        createKey(testBucket.delegate(), DUMMY_KEY, SizeInBytes.ONE_KB, SizeInBytes.ONE_KB);

        for (SizeInBytes bufferSize : bufferSizes) {
          // create key
          System.out.println("---------------------------------------------------------");
          createKey(testBucket.delegate(), keyName, keySize, bufferSize);

          // get block file and generate md5
          final OmKeyInfo info = nonStreamReadClient.getProxy().getKeyInfo(volume, bucket, keyName, false);
          final List<OmKeyLocationInfo> locations = info.getLatestVersionLocations().getLocationList();
          assertEquals(1, locations.size());
          final BlockID blockId = locations.get(0).getBlockID();
          final ContainerData containerData = datanode.getDatanodeStateMachine().getContainer().getContainerSet()
              .getContainer(blockId.getContainerID()).getContainerData();
          final File blockFile = ContainerLayoutVersion.FILE_PER_BLOCK.getChunkFile(containerData, blockId, null);
          assertTrue(blockFile.exists());
          assertEquals(BLOCK_SIZE, blockFile.length());
          final String expectedMd5 = generateMd5(keySize, SizeInBytes.ONE_MB, blockFile);

          // run tests
          System.out.println("---------------------------------------------------------");
          System.out.printf("%s with %s bytes and %s bytesPerChecksum%n",
              keyName, keySize, bytesPerChecksum);

          final CheckedBiConsumer<SizeInBytes, String, Exception> streamRead = (readBufferSize, md5)
              -> streamRead(keySize, readBufferSize, md5, testBucket, keyName);
          final CheckedBiConsumer<SizeInBytes, String, Exception> nonStreamRead = (readBufferSize, md5)
              -> nonStreamRead(keySize, readBufferSize, md5, nonStreamReadClient, volume, bucket, keyName);
          final CheckedBiConsumer<SizeInBytes, String, Exception> fileRead = (readBufferSize, md5)
              -> fileRead(keySize, readBufferSize, md5, blockFile);
          final List<CheckedBiConsumer<SizeInBytes, String, Exception>> operations
              = Arrays.asList(streamRead, nonStreamRead, fileRead);
          Collections.shuffle(operations);

          for (CheckedBiConsumer<SizeInBytes, String, Exception> op : operations) {
            for (int i = 0; i < 5; i++) {
              op.accept(bufferSize, null);
            }
            op.accept(bufferSize, expectedMd5);
          }
        }
      }
    }
  }

  static void streamRead(SizeInBytes keySize, SizeInBytes bufferSize, String expectedMD5,
      TestBucket bucket, String keyName) throws Exception {
    try (KeyInputStream in = bucket.getKeyInputStream(keyName)) {
      assertTrue(in.isStreamBlockInputStream());
      runTestReadKey(keySize, bufferSize, expectedMD5, in);
    }
  }

  static void nonStreamRead(SizeInBytes keySize, SizeInBytes bufferSize, String expectedMD5,
      OzoneClient nonStreamReadClient, String volume, String bucket, String keyName) throws Exception {
    final ClientProtocol proxy = nonStreamReadClient.getProxy();
    try (KeyInputStream in = (KeyInputStream) proxy.getKey(volume, bucket, keyName).getInputStream()) {
      assertFalse(in.isStreamBlockInputStream());
      runTestReadKey(keySize, bufferSize, expectedMD5, in);
    }
  }

  static void fileRead(SizeInBytes keySize, SizeInBytes bufferSize, String expectedMD5,
      File blockFile) throws Exception {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(blockFile.toPath()), bufferSize.getSizeInt())) {
      runTestReadKey(keySize, bufferSize, expectedMD5, in);
    }
  }

  static String generateMd5(SizeInBytes keySize, SizeInBytes bufferSize, File blockFile) throws Exception {
    try (InputStream in = new BufferedInputStream(Files.newInputStream(blockFile.toPath()), bufferSize.getSizeInt())) {
      return runTestReadKey("generateMd5", keySize, bufferSize, true, in);
    }
  }

  static void print(String name, long keySizeByte, long elapsedNanos, SizeInBytes bufferSize, String computedMD5) {
    final double keySizeMb = keySizeByte * 1.0 / (1 << 20);
    final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    if (computedMD5 == null) {
      System.out.printf("%16s: %8.2f MB/s (%7.3f s, buffer %16s, keySize %8.2f MB)%n",
          name, keySizeMb / elapsedSeconds, elapsedSeconds, bufferSize, keySizeMb);
    } else {
      System.out.printf("%16s md5=%s%n", name, computedMD5);
    }
  }

  static void createKey(OzoneBucket bucket, String keyName, SizeInBytes keySize, SizeInBytes bufferSize)
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
    if (!keyName.startsWith(DUMMY_KEY)) {
      print("createStreamKey", keySizeByte, elapsedNanos, bufferSize, null);
    }
  }

  static void runTestReadKey(SizeInBytes keySize, SizeInBytes bufferSize, String expectedMD5,
      InputStream in) throws Exception {
    final String method = JavaUtils.getCallerStackTraceElement().getMethodName();
    final String computedMD5 = runTestReadKey(method, keySize, bufferSize, expectedMD5 != null, in);
    assertEquals(expectedMD5, computedMD5);
  }

  static String runTestReadKey(String name, SizeInBytes keySize, SizeInBytes bufferSize, boolean generateMd5,
      InputStream in) throws Exception {
    final long keySizeByte = keySize.getSize();
    final MessageDigest md5 = MessageDigest.getInstance("MD5");
    // Read the data fully into a large enough byte array
    final byte[] buffer = new byte[bufferSize.getSizeInt()];
    final long startTime = System.nanoTime();
    int pos = 0;
    for (; pos < keySizeByte;) {
      final int read = in.read(buffer, 0, buffer.length);
      if (read == -1) {
        break;
      }

      if (generateMd5) {
        md5.update(buffer, 0, read);
      }
      pos += read;
    }
    assertEquals(keySizeByte, pos);
    final long elapsedNanos = System.nanoTime() - startTime;

    final String computedMD5 = generateMd5 ? StringUtils.bytes2Hex(md5.digest()) : null;
    print(name, keySizeByte, elapsedNanos, bufferSize, computedMD5);
    return computedMD5;
  }
}
