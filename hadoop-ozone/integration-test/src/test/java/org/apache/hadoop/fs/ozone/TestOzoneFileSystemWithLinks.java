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

import org.apache.commons.io.IOUtils;;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

import java.io.IOException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;


/**
 * Ozone file system tests for Link Buckets.
 */
public class TestOzoneFileSystemWithLinks {

  private static final float TRASH_INTERVAL = 0.05f; // 3 seconds

  public TestOzoneFileSystemWithLinks() throws Exception {
    try {
      teardown();
      init();
    } catch (Exception e) {
      LOG.info("Unexpected exception", e);
      fail("Unexpected exception:" + e.getMessage());
    }
  }


  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystem.class);

  private static BucketLayout bucketLayout = BucketLayout.DEFAULT;

  private static MiniOzoneCluster cluster;
  private static OzoneManagerProtocol writeClient;
  private static FileSystem fs;
  private static OzoneFileSystem o3fs;
  private static String volumeName;
  private static String bucketName;
  private static Trash trash;
  private OzoneConfiguration conf;

  private void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);

    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    writeClient = cluster.getRpcClient().getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(cluster, bucketLayout);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    fs = FileSystem.get(conf);
    trash = new Trash(conf);
    o3fs = (OzoneFileSystem) fs;
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @After
  public void cleanup() {
    try {
      Path root = new Path("/");
      FileStatus[] fileStatuses = fs.listStatus(root);
      for (FileStatus fileStatus : fileStatuses) {
        fs.delete(fileStatus.getPath(), true);
      }
    } catch (IOException ex) {
      fail("Failed to cleanup files.");
    }
  }

  public static MiniOzoneCluster getCluster() {
    return cluster;
  }

  public static FileSystem getFs() {
    return fs;
  }

  public static void setBucketLayout(BucketLayout bLayout) {
    bucketLayout = bLayout;
  }

  public static String getBucketName() {
    return bucketName;
  }

  public static String getVolumeName() {
    return volumeName;
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @Test
  public void testLoopInLinkBuckets() throws Exception {
    String linksVolume = UUID.randomUUID().toString();

    OzoneClient client = cluster.getClient();
    ObjectStore store = client.getObjectStore();

    // Create volume
    store.createVolume(linksVolume);
    OzoneVolume volume = store.getVolume(linksVolume);

    String linkBucket1Name = UUID.randomUUID().toString();
    String linkBucket2Name = UUID.randomUUID().toString();
    String linkBucket3Name = UUID.randomUUID().toString();

    // case-1: Create a loop in the link buckets
    createLinkBucket(volume, linkBucket1Name, linkBucket2Name);
    createLinkBucket(volume, linkBucket2Name, linkBucket3Name);
    createLinkBucket(volume, linkBucket3Name, linkBucket1Name);

    // Set the fs.defaultFS and start the filesystem

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, linkBucket1Name, linksVolume);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    try {
      FileSystem.get(conf);
      Assert.fail("Should throw Exception due to loop in Link Buckets");
    } catch (OMException oe) {
      // Expected exception
      Assert.assertEquals(OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS,
          oe.getResult());
    }

    // case-2: Dangling link bucket
    String danglingLinkBucketName = UUID.randomUUID().toString();
    String sourceBucketName = UUID.randomUUID().toString();

    // danglingLinkBucket is a dangling link over a source bucket that doesn't
    // exist.
    createLinkBucket(volume, sourceBucketName, danglingLinkBucketName);

    rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, danglingLinkBucketName, linksVolume);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    try {
      FileSystem.get(conf);
      Assert.fail("Should throw Exception due to loop in Link Buckets");
    } catch (OMException oe) {
      // Expected exception
      Assert.assertEquals(OMException.ResultCodes.BUCKET_NOT_FOUND,
          oe.getResult());
    }
  }

  /**
   * Helper method to create Link Buckets.
   *
   * @param sourceVolume Name of source volume for Link Bucket.
   * @param sourceBucket Name of source bucket for Link Bucket.
   * @param linkBucket   Name of Link Bucket
   * @throws IOException
   */
  private void createLinkBucket(OzoneVolume sourceVolume, String sourceBucket,
                                String linkBucket) throws IOException {
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setBucketLayout(BucketLayout.DEFAULT)
        .setSourceVolume(sourceVolume.getName())
        .setSourceBucket(sourceBucket);
    sourceVolume.createBucket(linkBucket, builder.build());
  }
}
