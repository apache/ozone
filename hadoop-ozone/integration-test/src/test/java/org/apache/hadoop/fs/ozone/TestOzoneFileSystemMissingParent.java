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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests OFS behavior when filesystem paths are enabled and parent directory is
 * missing for some reason.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneFileSystemMissingParent implements NonHATests.TestCase {

  private Path bucketPath;
  private FileSystem fs;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    client = cluster().newClient();

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    String volumeName = bucket.getVolumeName();
    Path volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    String bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);

    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME,
        cluster().getConf().get(OZONE_OM_ADDRESS_KEY));

    fs = FileSystem.get(URI.create(rootPath), cluster().getConf());
  }

  @AfterEach
  public void cleanUp() throws Exception {
    fs.delete(bucketPath, true);
  }

  @AfterAll
  void tearDown() {
    IOUtils.closeQuietly(client, fs);
  }

  /**
   * Test if the parent directory gets deleted before commit.
   */
  @Test
  public void testCloseFileWithDeletedParent() throws Exception {
    // Test if the parent directory gets deleted before commit.
    Path parent = new Path(bucketPath, "parent");
    Path file = new Path(parent, "file");

    // Create file with missing parent, this would create parent directory.
    FSDataOutputStream stream = fs.create(file);

    // Delete the parent.
    fs.delete(parent, false);

    // Close should throw exception, Since parent doesn't exist.
    OMException omException = assertThrows(OMException.class, stream::close);
    assertThat(omException.getMessage())
        .contains("Cannot create file : " + "parent/file as parent directory doesn't exist");
  }

  /**
   * Test if the parent directory gets renamed before commit.
   */
  @Test
  public void testCloseFileWithRenamedParent() throws Exception {
    Path parent = new Path(bucketPath, "parent");
    Path file = new Path(parent, "file");

    // Create file with missing parent, this would create parent directory.
    FSDataOutputStream stream = fs.create(file);

    // Rename the parent to some different path.
    Path renamedPath = new Path(bucketPath, "parent1");
    fs.rename(parent, renamedPath);

    // Close should throw exception, Since parent has been moved.
    OMException omException = assertThrows(OMException.class, stream::close);
    assertThat(omException.getMessage())
        .contains("Cannot create file : " + "parent/file as parent directory doesn't exist");

    fs.delete(renamedPath, true);
  }
}
