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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.Rule;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.Timeout;

import java.io.IOException;

import static org.apache.hadoop.hdds.StringUtils.string2Bytes;

/**
 * Test OM Metrics for OzoneFileSystem operations.
 */
public class TestOzoneFileSystemMetrics {
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static OzoneBucket bucket;

  enum TestOps {
    File,
    Directory,
    Key
  }
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
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setChunkSize(2) // MB
        .setBlockSize(8) // MB
        .setStreamBufferFlushSize(2) // MB
        .setStreamBufferMaxSize(4) // MB
        .build();
    cluster.waitForClusterToBeReady();

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(cluster);

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri);
    fs =  FileSystem.get(conf);
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
  public void testKeyOps() throws Exception {
    testOzoneFileCommit(TestOps.Key);
  }

  @Test
  public void testFileOps() throws Exception {
    testOzoneFileCommit(TestOps.File);
  }

  @Test
  public void testDirOps() throws Exception {
    testOzoneFileCommit(TestOps.Directory);
  }

  private void testOzoneFileCommit(TestOps op) throws Exception {
    long numKeysBeforeCreate = cluster
        .getOzoneManager().getMetrics().getNumKeys();

    int fileLen = 30 * 1024 * 1024;
    byte[] data = string2Bytes(RandomStringUtils.randomAlphanumeric(fileLen));

    Path parentDir = new Path("/" + RandomStringUtils.randomAlphanumeric(5));
    Path filePath = new Path(parentDir,
        RandomStringUtils.randomAlphanumeric(5));

    switch (op) {
    case Key:
      try (OzoneOutputStream stream =
               bucket.createKey(filePath.toString(), fileLen)) {
        stream.write(data);
      }
      break;
    case File:
      try (FSDataOutputStream stream = fs.create(filePath)) {
        stream.write(data);
      }
      break;
    case Directory:
      fs.mkdirs(filePath);
      break;
    default:
      throw new IOException("Execution should never reach here." + op);
    }

    long numKeysAfterCommit = cluster
        .getOzoneManager().getMetrics().getNumKeys();
    Assert.assertTrue(numKeysAfterCommit > 0);
    Assert.assertEquals(numKeysBeforeCreate + 2, numKeysAfterCommit);
    fs.delete(parentDir, true);

    long numKeysAfterDelete = cluster
        .getOzoneManager().getMetrics().getNumKeys();
    Assert.assertTrue(numKeysAfterDelete >= 0);
    Assert.assertEquals(numKeysBeforeCreate, numKeysAfterDelete);
  }
}
