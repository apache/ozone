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

import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test OM Metrics for OzoneFileSystem operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFileSystemMetrics implements NonHATests.TestCase {

  private OzoneClient client;
  private FileSystem fs;
  private OzoneBucket bucket;
  private OmConfig originalOmConfig;

  enum TestOps {
    File,
    Directory,
    Key
  }

  @BeforeAll
  void init() throws Exception {
    cluster().getOzoneManager().getKeyManager().getDeletingService().suspend();
    cluster().getOzoneManager().getKeyManager().getDirDeletingService().suspend();

    client = cluster().newClient();

    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    originalOmConfig = omConfig.copy();
    omConfig.setFileSystemPathEnabled(true);

    // create a volume and a bucket to be used by OzoneFileSystem
    bucket = TestDataUtil.createVolumeAndBucket(client, BucketLayout.LEGACY);

    // Set the fs.defaultFS and start the filesystem
    String uri = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    fs =  FileSystem.get(URI.create(uri), cluster().getConf());
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client, fs);
    cluster().getOzoneManager().getConfig().setFrom(originalOmConfig);
    cluster().getOzoneManager().getKeyManager().getDeletingService().resume();
    cluster().getOzoneManager().getKeyManager().getDirDeletingService().resume();
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
    long numKeysBeforeCreate = cluster()
        .getOzoneManager().getMetrics().getNumKeys();

    int fileLen = 30 * 1024 * 1024;
    byte[] data = string2Bytes(RandomStringUtils.secure().nextAlphanumeric(fileLen));

    Path parentDir = new Path("/" + RandomStringUtils.secure().nextAlphanumeric(5));
    Path filePath = new Path(parentDir,
        RandomStringUtils.secure().nextAlphanumeric(5));

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

    long numKeysAfterCommit = cluster()
        .getOzoneManager().getMetrics().getNumKeys();
    assertThat(numKeysAfterCommit).isGreaterThan(0);
    assertEquals(numKeysBeforeCreate + 2, numKeysAfterCommit);
    fs.delete(parentDir, true);

    long numKeysAfterDelete = cluster()
        .getOzoneManager().getMetrics().getNumKeys();
    assertThat(numKeysAfterDelete).isGreaterThanOrEqualTo(0);
    assertEquals(numKeysBeforeCreate, numKeysAfterDelete);
  }
}
