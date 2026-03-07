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
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that when DataNode-side datastream is disabled, the client fails
 * fast instead of falling back to the gRPC port for streaming writes.
 */
public class TestOzoneFileSystemWithStreamingDisabledDatanode {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    // DataNode side: disable datastream, so pipeline should not explicitly
    // carry RATIS_DATASTREAM ports.
    conf.setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);

    // Client side: enable datastream so the write path will attempt streaming.
    conf.setBoolean(OZONE_FS_DATASTREAM_ENABLED, true);

    // Force datastream path deterministically.
    conf.set(OZONE_FS_DATASTREAM_AUTO_THRESHOLD, "1B");

    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setChunkSize(16 << 10)
        .setStreamBufferFlushSize(32 << 10)
        .setStreamBufferMaxSize(64 << 10)
        .setBlockSize(128 << 10)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatastreamWriteFailsFastWhenDatanodeStreamingDisabled()
      throws Exception {
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final byte[] bytes = new byte[3 << 20];
    ThreadLocalRandom.current().nextBytes(bytes);

    try (FileSystem fs = FileSystem.get(conf)) {
      final Path file = new Path("/streaming-disabled-dn.dat");

      IOException ex = assertThrows(IOException.class, () -> {
        try (FSDataOutputStream out = fs.create(file, true)) {
          out.write(bytes);
        }
      });

      final String msg = collectMessages(ex).toLowerCase();

      // Expect a clear fail-fast error from our validation.
      assertTrue(
          msg.contains("ratis_datastream port is missing")
              || msg.contains("datastream is disabled")
              || msg.contains("ratis_datastream"),
          () -> "Expected a clear fail-fast datastream error, but got: " + ex);

      // Ensure we did not fall back to the old gRPC/HTTP2 failure path.
      assertTrue(
          !msg.contains("http/2") && !msg.contains("timeout"),
          () -> "Should not fall back to gRPC/HTTP2 path, but got: " + ex);
    }
  }

  private static String collectMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    while (t != null) {
      if (t.getMessage() != null) {
        sb.append(t.getMessage()).append('\n');
      }
      t = t.getCause();
    }
    return sb.toString();
  }
}
