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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.Assert.fail;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
@RunWith(Parameterized.class)
public class TestOzoneFileSystem {

  private static final float TRASH_INTERVAL = 0.05f; // 3 seconds

  private static final Path ROOT =
      new Path(OZONE_URI_DELIMITER);

  private static final Path TRASH_ROOT =
      new Path(ROOT, TRASH_PREFIX);

  private static final PathFilter EXCLUDE_TRASH =
      p -> !p.toUri().getPath().startsWith(TRASH_ROOT.toString());

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestOzoneFileSystem(boolean setDefaultFs, boolean enableOMRatis) {
    // Checking whether 'defaultFS' and 'omRatis' flags represents next
    // parameter index values. This is to ensure that initialize
    // TestOzoneFileSystem#init() function will be invoked only at the
    // beginning of every new set of Parameterized.Parameters.
    if (enabledFileSystemPaths != setDefaultFs ||
            omRatisEnabled != enableOMRatis || cluster == null) {
      enabledFileSystemPaths = setDefaultFs;
      omRatisEnabled = enableOMRatis;
      try {
        teardown();
        init();
      } catch (Exception e) {
        LOG.info("Unexpected exception", e);
        fail("Unexpected exception:" + e.getMessage());
      }
    }
  }

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneFileSystem.class);

  private static BucketLayout bucketLayout = BucketLayout.LEGACY;
  private static boolean enabledFileSystemPaths;
  private static boolean omRatisEnabled;

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneManagerProtocol writeClient;
  private static FileSystem fs;
  private static OzoneFileSystem o3fs;
  private static OzoneBucket ozoneBucket;
  private static String volumeName;
  private static String bucketName;
  private static Trash trash;

  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);

    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    if (!bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
          enabledFileSystemPaths);
    }
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(5)
            .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();

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
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @After
  public void cleanup() {
    try {
      deleteRootDir();
    } catch (IOException | InterruptedException | TimeoutException ex) {
      LOG.error("Failed to cleanup files.", ex);
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

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  protected void deleteRootDir()
      throws IOException, InterruptedException, TimeoutException {
    FileStatus[] fileStatuses = fs.listStatus(ROOT);

    if (fileStatuses == null) {
      return;
    }
    deleteRootRecursively(fileStatuses);
    GenericTestUtils.waitFor(() -> {
      FileStatus[] fileStatus = new FileStatus[0];
      try {
        fileStatus = fs.listStatus(ROOT);
        return fileStatus != null && fileStatus.length == 0;
      } catch (IOException e) {
        Assert.assertFalse(fileStatus.length == 0);
        return false;
      }
    }, 100, 500);
  }

  private static void deleteRootRecursively(FileStatus[] fileStatuses)
      throws IOException {
    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }
  }

  private void listStatusIterator(int numDirs) throws IOException {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Set<String> paths = new TreeSet<>();
    try {
      for (int i = 0; i < numDirs; i++) {
        Path p = new Path(root, String.valueOf(i));
        fs.mkdirs(p);
        paths.add(p.getName());
      }

      RemoteIterator<FileStatus> iterator = o3fs.listStatusIterator(root);
      int iCount = 0;
      if (iterator != null) {
        while (iterator.hasNext()) {
          FileStatus fileStatus = iterator.next();
          iCount++;
          Assert.assertTrue(paths.contains(fileStatus.getPath().getName()));
        }
      }
      Assert.assertEquals(
          "Total directories listed do not match the existing directories",
          numDirs, iCount);

    } finally {
      // Cleanup
      for (int i = 0; i < numDirs; i++) {
        Path p = new Path(root, String.valueOf(i));
        fs.delete(p, true);
      }
    }
  }

  private OzoneKeyDetails getKey(Path keyPath, boolean isDirectory)
      throws IOException {
    String key = o3fs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + "/";
    }
    return client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(key);
  }

  private void assertKeyNotFoundException(IOException ex) {
    GenericTestUtils.assertExceptionContains("KEY_NOT_FOUND", ex);
  }

  private void createKeyAndAssertKeyType(OzoneBucket bucket,
      OzoneFileSystem o3FS, Path keyPath, ReplicationType expectedType)
      throws IOException {
    o3FS.createFile(keyPath).build().close();
    Assert.assertEquals(expectedType.name(),
        bucket.getKey(o3FS.pathToKey(keyPath)).getReplicationConfig()
            .getReplicationType().name());
  }

  /**
   * Check that files are moved to trash.
   * since fs.rename(src,dst,options) is enabled.
   */
  @Test
  public void testRenameToTrashEnabled() throws Exception {
    // Create a file
    String testKeyName = "testKey1";
    Path path = new Path(OZONE_URI_DELIMITER, testKeyName);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }

    // Construct paths
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userTrash = new Path(TRASH_ROOT, username);

    Assert.assertFalse(o3fs.exists(userTrash));
    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(path);
    // We can safely assert only trash directory here.
    // Asserting Current or checkpoint directory is not feasible here in this
    // test due to independent TrashEmptier thread running in cluster and
    // possible flakiness is hard to avoid unless we test this test case
    // in separate mini cluster with closer accuracy of TrashEmptier.
    Assert.assertTrue(o3fs.exists(userTrash));
  }

}
