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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.junit.After;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test verifies the entries and operations in prefix table.
 */
public class TestOzonePrefixTable {

  @Rule
  public Timeout timeout = new Timeout(300000);

  private static final Logger LOG =
          LoggerFactory.getLogger(TestOzoneFileSystem.class);

  private MiniOzoneCluster cluster;
  private FileSystem fs;
  private OzoneFileSystem o3fs;
  private String volumeName;
  private String bucketName;
  private int rootItemCount;

  @Test(timeout = 300_000)
  public void testMultiLevelDirs() throws Exception {
    setupOzoneFileSystem();
    // Op 1. create dir -> /d1/d2/d3/d4/
    // Op 2. create dir -> /d1/d2/d3/d4/d5
    // Op 3. create dir -> /d1/d2/d3/d4/d6
    Path parent = new Path("/d1/d2/d3/d4/");
    fs.mkdirs(parent);

    OMMetadataManager omMgr = cluster.getOzoneManager().getMetadataManager();
    OmBucketInfo omBucketInfo = omMgr.getBucketTable().get(
            omMgr.getBucketKey(volumeName, bucketName));
    Assert.assertNotNull("Failed to find bucketInfo", omBucketInfo);

    ArrayList<String> prefixKeys = new ArrayList<>();
    long d1ObjectID = verifyPrefixKey(omBucketInfo.getObjectID(), "d1", "/d1",
            prefixKeys, omMgr);
    long d2ObjectID = verifyPrefixKey(d1ObjectID, "d2", "/d1/d2", prefixKeys,
            omMgr);
    long d3ObjectID = verifyPrefixKey(d2ObjectID, "d3", "/d1/d2/d3",
            prefixKeys, omMgr);
    long d4ObjectID = verifyPrefixKey(d3ObjectID, "d4", "/d1/d2/d3/d4",
            prefixKeys, omMgr);

    // verify entries in prefix table
    TableIterator<String, ? extends
            Table.KeyValue<String, OmPrefixInfo>> iterator =
            omMgr.getPrefixTable().iterator();
    int count = prefixKeys.size();
    Assert.assertEquals("Unexpected prefix table entries!", 4, count);
    while (iterator.hasNext()) {
      count--;
      Table.KeyValue<String, OmPrefixInfo> value = iterator.next();
      prefixKeys.remove(value.getKey());
    }
    Assert.assertEquals("Unexpected prefix table entries!", 0, count);

    // verify entries in key table
    TableIterator<String, ? extends
            Table.KeyValue<String, OmKeyInfo>> keyTableItr =
            omMgr.getKeyTable().iterator();
    while (keyTableItr.hasNext()) {
      fail("Shouldn't add any entries in KeyTable!");
    }

    // create sub-dirs under same parent
    Path subDir5 = new Path("/d1/d2/d3/d4/d5");
    fs.mkdirs(subDir5);
    Path subDir6 = new Path("/d1/d2/d3/d4/d6");
    fs.mkdirs(subDir6);
    long d5ObjectID = verifyPrefixKey(d4ObjectID, "d5", "/d1/d2/d3/d4/d5",
            prefixKeys, omMgr);
    long d6ObjectID = verifyPrefixKey(d4ObjectID, "d6", "/d1/d2/d3/d4/d6",
            prefixKeys, omMgr);
    Assert.assertTrue("Wrong objectIds for sub-dirs[" + d5ObjectID + "/d5, "
                    + d6ObjectID + "/d6] of same parent!",
            d5ObjectID != d6ObjectID);
  }

  long verifyPrefixKey(long parentId, String prefixKey, String absolutePath,
                       ArrayList<String> prefixKeys, OMMetadataManager omMgr)
          throws Exception {
    String dbKey = parentId + "/" + prefixKey;
    prefixKeys.add(dbKey);
    OmPrefixInfo prefixInfo = omMgr.getPrefixTable().get(dbKey);
    Assert.assertNotNull("Failed to find " + absolutePath +
            " using dbKey: " + dbKey, prefixInfo);
    Assert.assertEquals("Parent Id mismatches", parentId,
            prefixInfo.getParentObjectID());
    Assert.assertEquals("Mismatches prefix name", prefixKey,
            prefixInfo.getName());
    Assert.assertEquals("Mismatches prefix vol name", volumeName,
            prefixInfo.getVolumeName());
    Assert.assertEquals("Mismatches prefix bucket name", bucketName,
            prefixInfo.getBucketName());
    Assert.assertTrue("Mismatches prefix creation time param",
            prefixInfo.getCreationTime() > 0);
    Assert.assertEquals("Mismatches prefix modification time param",
            prefixInfo.getCreationTime(), prefixInfo.getModificationTime());
    return prefixInfo.getObjectID();
  }

  private void setupOzoneFileSystem()
          throws IOException, TimeoutException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 1);
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

}
