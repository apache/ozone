/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;

/**
 * Test verifies the entries and operations in file table, open file table etc.
 */
public class TestOzoneFileOps {

  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
          LoggerFactory.getLogger(TestOzoneFileOps.class);

  private MiniOzoneCluster cluster;
  private FileSystem fs;
  private String volumeName;
  private String bucketName;

  @Before
  public void setupOzoneFileSystem()
          throws IOException, TimeoutException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 1);
    TestOMRequestUtils.configureFSOptimizedPaths(conf,
            true, OMConfigKeys.OZONE_OM_LAYOUT_VERSION_V1);
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(3)
            .build();
    cluster.waitForClusterToBeReady();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
            OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
            bucket.getVolumeName());

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    fs = FileSystem.get(conf);
  }

  @After
  public void tearDown() {
    IOUtils.closeQuietly(fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 300_000)
  public void testCreateFile() throws Exception {
    // Op 1. create dir -> /d1/d2/d3/d4/
    Path parent = new Path("/d1/d2/");
    Path file = new Path(parent, "file1");
    FSDataOutputStream outputStream = fs.create(file);
    String openFileKey = "";

    OMMetadataManager omMgr = cluster.getOzoneManager().getMetadataManager();
    OmBucketInfo omBucketInfo = omMgr.getBucketTable().get(
            omMgr.getBucketKey(volumeName, bucketName));
    Assert.assertNotNull("Failed to find bucketInfo", omBucketInfo);

    ArrayList<String> dirKeys = new ArrayList<>();
    long d1ObjectID = verifyDirKey(omBucketInfo.getObjectID(), "d1", "/d1",
            dirKeys, omMgr);
    long d2ObjectID = verifyDirKey(d1ObjectID, "d2", "/d1/d2", dirKeys,
            omMgr);
    openFileKey = d2ObjectID + OzoneConsts.OM_KEY_PREFIX + file.getName();

    // trigger CommitKeyRequest
    outputStream.close();

    OmKeyInfo omKeyInfo = omMgr.getKeyTable().get(openFileKey);
    Assert.assertNotNull("Invalid Key!", omKeyInfo);
    verifyOMFileInfoFormat(omKeyInfo, file.getName(), d2ObjectID);

    // wait for DB updates
    GenericTestUtils.waitFor(() -> {
      try {
        return omMgr.getOpenKeyTable().isEmpty();
      } catch (IOException e) {
        LOG.error("DB failure!", e);
        Assert.fail("DB failure!");
        return false;
      }
    }, 1000, 120000);
  }

  private void verifyOMFileInfoFormat(OmKeyInfo omKeyInfo, String fileName,
                                      long parentID) {
    Assert.assertEquals("Wrong keyName", fileName,
            omKeyInfo.getKeyName());
    Assert.assertEquals("Wrong parentID", parentID,
            omKeyInfo.getParentObjectID());
    String dbKey = parentID + OzoneConsts.OM_KEY_PREFIX + fileName;
    Assert.assertEquals("Wrong path format", dbKey,
            omKeyInfo.getPath());
  }

  long verifyDirKey(long parentId, String dirKey, String absolutePath,
                    ArrayList<String> dirKeys, OMMetadataManager omMgr)
          throws Exception {
    String dbKey = parentId + OzoneConsts.OM_KEY_PREFIX + dirKey;
    dirKeys.add(dbKey);
    OmDirectoryInfo dirInfo = omMgr.getDirectoryTable().get(dbKey);
    Assert.assertNotNull("Failed to find " + absolutePath +
            " using dbKey: " + dbKey, dirInfo);
    Assert.assertEquals("Parent Id mismatches", parentId,
            dirInfo.getParentObjectID());
    Assert.assertEquals("Mismatches directory name", dirKey,
            dirInfo.getName());
    Assert.assertTrue("Mismatches directory creation time param",
            dirInfo.getCreationTime() > 0);
    Assert.assertEquals("Mismatches directory modification time param",
            dirInfo.getCreationTime(), dirInfo.getModificationTime());
    return dirInfo.getObjectID();
  }

}
