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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

/**
 * Ozone file system tests to validate default bucket layout configuration
 * and behaviour.
 * TODO: Refactor this and TestOzoneFileSystem to reduce duplication.
 */
@RunWith(Parameterized.class)
public class TestOzoneFSBucketLayout {

  private static String defaultBucketLayout;
  private static MiniOzoneCluster cluster = null;
  private static ObjectStore objectStore;
  private BasicRootedOzoneClientAdapterImpl adapter;
  private static String rootPath;
  private static String volumeName;
  private static Path volumePath;

  private static final String INVALID_CONFIG = "INVALID";
  private static final Map<String, String> ERROR_MAP = new HashMap<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFSBucketLayout.class);

  // Initialize error map.
  static {
    ERROR_MAP.put(BucketLayout.OBJECT_STORE.name(),
        "Buckets created with OBJECT_STORE layout do not support file " +
            "system semantics.");
    ERROR_MAP.put(INVALID_CONFIG, "Unsupported value provided for " +
        OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT);
  }

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList(
        // Empty Config
        "",
        // Invalid Config
        INVALID_CONFIG,
        // Unsupported Bucket Layout for OFS
        BucketLayout.OBJECT_STORE.name(),
        // Supported bucket layouts.
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name(),
        BucketLayout.LEGACY.name()
    );
  }

  public TestOzoneFSBucketLayout(String bucketLayout) {
    // Ignored. Actual init done in initParam().
    // This empty constructor is still required to avoid argument exception.
  }

  @Parameterized.BeforeParam
  public static void initDefaultLayout(String bucketLayout) {
    defaultBucketLayout = bucketLayout;
    LOG.info("Default bucket layout: {}", defaultBucketLayout);
  }

  @BeforeClass
  public static void initCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = cluster.getClient().getObjectStore();
    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    volumeName =
        TestDataUtil.createVolumeAndBucket(cluster)
            .getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
  }

  @AfterClass
  public static void teardown() throws IOException {
    // Tear down the cluster after EACH set of parameters
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileSystemBucketLayoutConfiguration() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setFsDefaultBucketLayout(defaultBucketLayout);

    conf.setFromObject(clientConfig);

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);

    // In case OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT is set to OBS,
    // FS initialization should fail.

    if (ERROR_MAP.containsKey(defaultBucketLayout)) {
      try {
        FileSystem.newInstance(conf);
        Assert.fail("File System initialization should fail in case " +
            " of invalid configuration of " +
            OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT);
      } catch (OMException oe) {
        Assert.assertTrue(
            oe.getMessage().contains(ERROR_MAP.get(defaultBucketLayout)));
        return;
      }
    }

    // initialize FS and adapter.
    FileSystem fs = FileSystem.newInstance(conf);
    RootedOzoneFileSystem ofs = (RootedOzoneFileSystem) fs;
    adapter = (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();

    // Create a new directory, which in turn creates a new bucket.
    Path root = new Path("/" + volumeName);

    String bucketName = getBucketName();
    Path dir1 = new Path(root, bucketName);

    adapter.createDirectory(dir1.toString());

    // Make sure the bucket layout of created bucket matches the config.
    OzoneBucket bucketInfo =
        objectStore.getClientProxy().getBucketDetails(volumeName, bucketName);
    if (StringUtils.isNotBlank(defaultBucketLayout)) {
      Assert.assertEquals(defaultBucketLayout,
          bucketInfo.getBucketLayout().name());
    } else {
      Assert.assertEquals(OzoneConfigKeys.OZONE_CLIENT_FS_BUCKET_LAYOUT_DEFAULT,
          bucketInfo.getBucketLayout().name());
    }

    // cleanup
    IOUtils.closeQuietly(fs);
  }

  private String getBucketName() {
    String bucketSuffix;
    if (StringUtils.isNotBlank(defaultBucketLayout)) {
      bucketSuffix = defaultBucketLayout
          .toLowerCase()
          .replaceAll("_", "-");
    } else {
      bucketSuffix = "empty";
    }

    return "bucket-" + bucketSuffix;
  }
}
