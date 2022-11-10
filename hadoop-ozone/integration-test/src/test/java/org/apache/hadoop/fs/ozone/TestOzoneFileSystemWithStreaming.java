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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.fail;

/**
 * Ozone file system tests with Streaming.
 */
public class TestOzoneFileSystemWithStreaming {
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static final Logger LOG
      = LoggerFactory.getLogger(TestOzoneFileSystemWithStreaming.class);

  private static final BucketLayout BUCKET_LAYOUT
      = BucketLayout.FILE_SYSTEM_OPTIMIZED;

  private static MiniOzoneCluster cluster;
  private static FileSystem fs;

  {
    try {
      init();
    } catch (Exception e) {
      LOG.info("Unexpected exception", e);
      fail("Unexpected exception:" + e.getMessage());
    }
  }

  private void init() throws Exception {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, false);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BUCKET_LAYOUT.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
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

    // create a volume and a bucket to be used by OzoneFileSystem
    final OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(
        cluster, BUCKET_LAYOUT);

    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testCreateFile() throws IOException {
    final byte[] bytes = new byte[1 << 20];
    ThreadLocalRandom.current().nextBytes(bytes);

    final Path file = new Path("/file");
    ContractTestUtils.createFile(fs, file, true, bytes);

    final byte[] buffer = new byte[4 << 10];
    int offset = 0;
    try (FSDataInputStream in = fs.open(file)) {
      for (; ;) {
        final int n = in.read(buffer, 0, buffer.length);
        if (n <= 0) {
          break;
        }
        for (int i = 0; i < n; i++) {
          Assertions.assertEquals(bytes[offset + i], buffer[i]);
        }
        offset += n;
      }
    }
    Assertions.assertEquals(bytes.length, offset);
  }
}
