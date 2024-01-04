/**
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

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests OFS behavior when filesystem paths are enabled and parent directory is
 * missing for some reason.
 */
public class TestOzoneFileSystemMissingParent {

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster;
  private static Path bucketPath;
  private static FileSystem fs;
  private static OzoneClient client;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        OMConfigKeys.OZONE_BUCKET_LAYOUT_FILE_SYSTEM_OPTIMIZED);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client);

    String volumeName = bucket.getVolumeName();
    Path volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    String bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);

    String rootPath = String
        .format("%s://%s/", OzoneConsts.OZONE_OFS_URI_SCHEME,
            conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and create filesystem.
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    fs.delete(bucketPath, true);
  }

  @AfterAll
  public static void tearDown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
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
    assertTrue(omException.getMessage().contains("Cannot create file : " +
        "parent/file as parent directory doesn't exist"));
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
    assertTrue(omException.getMessage().contains("Cannot create file : " +
        "parent/file as parent directory doesn't exist"));

    fs.delete(renamedPath, true);
  }
}
