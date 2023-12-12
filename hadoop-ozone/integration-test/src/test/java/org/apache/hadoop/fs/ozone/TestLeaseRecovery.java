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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_ROOT;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for recoverLease() API.
 */
@Timeout(300)
public class TestLeaseRecovery {

  private MiniOzoneCluster cluster;
  private OzoneBucket bucket;

  private OzoneClient client;
  private final OzoneConfiguration conf = new OzoneConfiguration();

  /**
   * Closing the output stream after lease recovery throws because the key
   * is no longer open in OM.  This is currently expected (see HDDS-9358).
   */
  public static void closeIgnoringKeyNotFound(OutputStream stream)
      throws IOException {
    try {
      stream.close();
    } catch (OMException e) {
      assertEquals(OMException.ResultCodes.KEY_NOT_FOUND, e.getResult());
    }
  }

  @BeforeEach
  public void init() throws IOException, InterruptedException,
      TimeoutException {
    final int chunkSize = 16 << 10;
    final int flushSize = 2 * chunkSize;
    final int maxFlushSize = 2 * flushSize;
    final int blockSize = 2 * maxFlushSize;
    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    conf.setBoolean(OZONE_OM_RATIS_ENABLE_KEY, false);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
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
    client = cluster.newClient();

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testRecovery() throws Exception {
    // Set the fs.defaultFS
    final String rootPath = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName()
        + OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, "file");

    RootedOzoneFileSystem fs = (RootedOzoneFileSystem)FileSystem.get(conf);

    final byte[] data = new byte[1 << 20];
    ThreadLocalRandom.current().nextBytes(data);

    final FSDataOutputStream stream = fs.create(file, true);
    try {
      stream.write(data);
      stream.hsync();
      assertFalse(fs.isFileClosed(file));

      int count = 0;
      while (count++ < 15 && !fs.recoverLease(file)) {
        Thread.sleep(1000);
      }
      // The lease should have been recovered.
      assertTrue(fs.recoverLease(file), "File should be closed");
      assertTrue(fs.isFileClosed(file));
    } finally {
      closeIgnoringKeyNotFound(stream);
    }

    // open it again, make sure the data is correct
    byte[] readData = new byte[1 << 20];
    try (FSDataInputStream fdis = fs.open(file)) {
      int readBytes = fdis.read(readData);
      assertEquals(readBytes, 1 << 20);
      assertArrayEquals(readData, data);
    }
  }

  @Test
  public void testOBSRecoveryShouldFail() throws Exception {
    // Set the fs.defaultFS
    bucket = TestDataUtil.createVolumeAndBucket(client,
        "vol2", "obs", BucketLayout.OBJECT_STORE);
    final String rootPath = String.format("%s://%s/", OZONE_OFS_URI_SCHEME,
        conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    final String dir = OZONE_ROOT + bucket.getVolumeName() +
        OZONE_URI_DELIMITER + bucket.getName();
    final Path file = new Path(dir, "file");

    RootedOzoneFileSystem fs = (RootedOzoneFileSystem) FileSystem.get(conf);
    assertThrows(IllegalArgumentException.class, () -> fs.recoverLease(file));
  }
}
