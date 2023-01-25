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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Ozone file system tests with Streaming.
 */
@Timeout(value = 300)
public class TestOzoneFileSystemWithStreaming {

  private static MiniOzoneCluster cluster;
  private static OzoneBucket bucket;

  private static OzoneConfiguration conf = new OzoneConfiguration();

  @BeforeAll
  public static void init() throws Exception {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    conf.setBoolean(DFS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OZONE_FS_DATASTREAM_ENABLED, true);
    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    conf.set(OZONE_DEFAULT_BUCKET_LAYOUT, layout.name());
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
    bucket = TestDataUtil.createVolumeAndBucket(cluster, layout);
  }

  @AfterAll
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testO3fsCreateFile() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final Path file = new Path("/file");

    try (FileSystem fs = FileSystem.get(conf)) {
      runTestCreateFile(fs, file);
    }
  }

  @Test
  public void testOfsCreateFile() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, "file");

    try (FileSystem fs = FileSystem.get(conf)) {
      runTestCreateFile(fs, file);
    }
  }

  static void runTestCreateFile(FileSystem fs, Path file) throws Exception {
    final byte[] bytes = new byte[1 << 20];
    ThreadLocalRandom.current().nextBytes(bytes);

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
          assertEquals(bytes[offset + i], buffer[i]);
        }
        offset += n;
      }
    }
    assertEquals(bytes.length, offset);
  }
}
