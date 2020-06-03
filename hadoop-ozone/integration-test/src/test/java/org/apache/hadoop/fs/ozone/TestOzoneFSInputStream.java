/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.hadoop.hdds.StringUtils.string2Bytes;

/**
 * Test OzoneFSInputStream by reading through multiple interfaces.
 */
public class TestOzoneFSInputStream {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = new Timeout(300000);
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static Path filePath = null;
  private static byte[] data = null;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setChunkSize(2) // MB
        .setBlockSize(8) // MB
        .setStreamBufferFlushSize(2) // MB
        .setStreamBufferMaxSize(4) // MB
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri);
    fs =  FileSystem.get(conf);
    int fileLen = 30 * 1024 * 1024;
    data = string2Bytes(RandomStringUtils.randomAlphanumeric(fileLen));
    filePath = new Path("/" + RandomStringUtils.randomAlphanumeric(5));
    try (FSDataOutputStream stream = fs.create(filePath)) {
      stream.write(data);
    }
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    fs.close();
    cluster.shutdown();
  }

  @Test
  public void testO3FSSingleByteRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      byte[] value = new byte[data.length];
      int i = 0;
      while (true) {
        int val = inputStream.read();
        if (val == -1) {
          break;
        }
        value[i] = (byte) val;
        Assert.assertEquals("value mismatch at:" + i, value[i], data[i]);
        i++;
      }
      Assert.assertEquals(i, data.length);
      Assert.assertArrayEquals(value, data);
    }
  }

  @Test
  public void testO3FSMultiByteRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {
      byte[] value = new byte[data.length];
      byte[] tmp = new byte[1 * 1024 * 1024];
      int i = 0;
      while (true) {
        int val = inputStream.read(tmp);
        if (val == -1) {
          break;
        }
        System.arraycopy(tmp, 0, value, i * tmp.length, tmp.length);
        i++;
      }
      Assert.assertEquals(i * tmp.length, data.length);
      Assert.assertArrayEquals(value, data);
    }
  }

  @Test
  public void testO3FSByteBufferRead() throws IOException {
    try (FSDataInputStream inputStream = fs.open(filePath)) {

      ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
      int byteRead = inputStream.read(buffer);

      Assert.assertEquals(byteRead, 1024 * 1024);

      byte[] value = new byte[1024 * 1024];
      System.arraycopy(data, 0, value, 0, value.length);

      Assert.assertArrayEquals(value, buffer.array());
    }
  }
}
