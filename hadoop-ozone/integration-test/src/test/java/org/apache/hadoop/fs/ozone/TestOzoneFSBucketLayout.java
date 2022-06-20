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
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.TrashPolicyOzone;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;

import java.util.Arrays;
import java.util.Collection;

/**
 * Ozone file system tests to validate default bucket layout configuration
 * and behaviour.
 * TODO: Refactor this and TestOzoneFileSystem to reduce duplication.
 */
@RunWith(Parameterized.class)
public class TestOzoneFSBucketLayout {

  private static BucketLayout defaultBucketLayout;

  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static ObjectStore objectStore;
  private static BasicRootedOzoneClientAdapterImpl adapter;

  private static String volumeName;
  private static Path volumePath;

  @Parameterized.Parameters
  public static Collection<BucketLayout> data() {
    return Arrays.asList(
        null,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        BucketLayout.LEGACY,
        BucketLayout.OBJECT_STORE
    );
  }

  public TestOzoneFSBucketLayout(BucketLayout bucketLayout) {
    // Ignored. Actual init done in initParam().
    // This empty constructor is still required to avoid argument exception.
  }

  @Parameterized.BeforeParam
  public static void initParam(BucketLayout bucketLayout)
      throws IOException, InterruptedException, TimeoutException {
    // Initialize the cluster before EACH set of parameters
    defaultBucketLayout = bucketLayout;

    initClusterAndEnv();
  }

  @Parameterized.AfterParam
  public static void teardownParam() {
    // Tear down the cluster after EACH set of parameters
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Before
  public void createVolumeAndBucket() throws IOException {
    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    TestOzoneFSBucketLayout.volumeName =
        TestDataUtil.createVolumeAndBucket(cluster)
            .getVolumeName();
    TestOzoneFSBucketLayout.volumePath =
        new Path(OZONE_URI_DELIMITER, volumeName);
  }

  @After
  public void cleanup() throws IOException {
    if (defaultBucketLayout != null &&
        defaultBucketLayout.equals(BucketLayout.OBJECT_STORE)) {
      return;
    }
    fs.delete(volumePath, true);
  }

  public static FileSystem getFs() {
    return fs;
  }


  public static void initClusterAndEnv() throws IOException,
      InterruptedException, TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    if (defaultBucketLayout != null) {
      clientConfig.setFsDefaultBucketLayout(defaultBucketLayout.name());
    } else {
      clientConfig.setFsDefaultBucketLayout("");
    }

    conf.setFromObject(clientConfig);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = cluster.getClient().getObjectStore();
    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);


    // In case OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT is set to OBS,
    // FS initialization should fail.
    if (defaultBucketLayout != null &&
        defaultBucketLayout.equals(BucketLayout.OBJECT_STORE)) {
      try {
        fs = FileSystem.get(conf);
        fail("File System initialization should fail in case " +
            BucketLayout.OBJECT_STORE + " is used as the default value for " +
            OzoneConfigKeys.OZONE_CLIENT_FS_DEFAULT_BUCKET_LAYOUT);
      } catch (OMException oe) {
        Assert.assertTrue(oe.getMessage().contains(
            "Buckets created with OBJECT_STORE layout do not support file " +
                "system semantics."));
        return;
      }
    }

    // fs.ofs.impl would be loaded from META-INF, no need to manually set it
    fs = FileSystem.get(conf);
    conf.setClass("fs.trash.classname", TrashPolicyOzone.class,
        TrashPolicy.class);
    RootedOzoneFileSystem ofs = (RootedOzoneFileSystem) fs;
    adapter = (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();
  }

  @Test
  public void testClientFsDefaultBucketLayout() throws Exception {
    if (defaultBucketLayout != null &&
        defaultBucketLayout.equals(BucketLayout.OBJECT_STORE)) {
      // We do not need to test with OBS. FS initialization fails in this case.
      return;
    }

    Path root = new Path("/" + volumeName);

    String bucketName = getBucketName();
    Path dir1 = new Path(root, bucketName);

    adapter.createDirectory(dir1.toString());

    OzoneBucket bucketInfo =
        objectStore.getClientProxy().getBucketDetails(volumeName, bucketName);

    if (defaultBucketLayout != null) {
      // Make sure the bucket layout matches the config.
      Assert.assertEquals(defaultBucketLayout, bucketInfo.getBucketLayout());
    } else {
      // In case we provide an empty config, the File System should use the
      // default config.
      Assert.assertEquals(
          "FS should use default config value in case of empty config",
          OzoneConfigKeys.OZONE_CLIENT_FS_BUCKET_LAYOUT_DEFAULT,
          bucketInfo.getBucketLayout().toString());
    }
  }

  private String getBucketName() {
    String bucketSuffix;
    if (defaultBucketLayout != null) {
      bucketSuffix = defaultBucketLayout
          .toString()
          .toLowerCase()
          .replaceAll("_", "-");
    } else {
      bucketSuffix = "null";
    }

    return "bucket-" + bucketSuffix;
  }
}
