/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * OmSnapshot file system tests.
 */
@RunWith(Parameterized.class)
public class TestOmSnapshotFileSystem {
  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String volumeName;
  private static String bucketName;
  private static FileSystem fs;
  private static OzoneFileSystem o3fs;
  private static OzoneManagerProtocol writeClient;
  private static BucketLayout bucketLayout;
  private static boolean enabledFileSystemPaths;
  private static File metaDir;
  private static OzoneManager ozoneManager;
  private static String keyPrefix;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestOmSnapshot.class);



  @Rule
  public Timeout timeout = new Timeout(120, TimeUnit.SECONDS);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{BucketLayout.FILE_SYSTEM_OPTIMIZED, false},
        new Object[]{BucketLayout.LEGACY, true});
  }

  public TestOmSnapshotFileSystem(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths) throws Exception {
    // Checking whether 'newBucketLayout' and
    // 'newEnableFileSystemPaths' flags represents next parameter
    // index values. This is to ensure that initialize init() function
    // will be invoked only at the beginning of every new set of
    // Parameterized.Parameters.
    if (TestOmSnapshotFileSystem.enabledFileSystemPaths !=
        newEnableFileSystemPaths ||
        TestOmSnapshotFileSystem.bucketLayout != newBucketLayout) {
      setConfig(newBucketLayout, newEnableFileSystemPaths);
      tearDown();
      init();
    }
  }
  private static void setConfig(BucketLayout newBucketLayout,
      boolean newEnableFileSystemPaths) {
    TestOmSnapshotFileSystem.enabledFileSystemPaths = newEnableFileSystemPaths;
    TestOmSnapshotFileSystem.bucketLayout = newBucketLayout;
  }
  /**
   * Create a MiniDFSCluster for testing.
   */
  private void init() throws Exception {
    conf = new OzoneConfiguration();
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    String omId = UUID.randomUUID().toString();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        enabledFileSystemPaths);
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();
    // create a volume and a bucket to be used by OzoneFileSystem
    OzoneBucket bucket = TestDataUtil
        .createVolumeAndBucket(cluster, bucketLayout);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();

    String rootPath = String
        .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
        bucket.getVolumeName());
    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    fs = FileSystem.get(conf);
    o3fs = (OzoneFileSystem) fs;

    OzoneClient client = cluster.getClient();
    ObjectStore objectStore = client.getObjectStore();
    writeClient = objectStore.getClientProxy().getOzoneManagerClient();
    ozoneManager = cluster.getOzoneManager();
    metaDir = OMStorage.getOmDbDir(conf);

    // stop the deletion services so that keys can still be read
    KeyManagerImpl keyManager = (KeyManagerImpl) ozoneManager.getKeyManager();
    keyManager.stop();
  }

  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  // based on TestObjectStoreWithFSO:testListKeysAtDifferentLevels
  public void testListKeysAtDifferentLevels() throws Exception {
    OzoneClient client = cluster.getClient();

    ObjectStore objectStore = client.getObjectStore();
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeName);
    Assert.assertTrue(ozoneVolume.getName().equals(volumeName));
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketName);
    Assert.assertTrue(ozoneBucket.getName().equals(bucketName));

    String keyc1 = "/a/b1/c1/c1.tx";
    String keyc2 = "/a/b1/c2/c2.tx";

    String keyd13 = "/a/b2/d1/d11.tx";
    String keyd21 = "/a/b2/d2/d21.tx";
    String keyd22 = "/a/b2/d2/d22.tx";
    String keyd31 = "/a/b2/d3/d31.tx";

    String keye11 = "/a/b3/e1/e11.tx";
    String keye21 = "/a/b3/e2/e21.tx";
    String keye31 = "/a/b3/e3/e31.tx";

    LinkedList<String> keys = new LinkedList<>();
    keys.add(keyc1);
    keys.add(keyc2);

    keys.add(keyd13);
    keys.add(keyd21);
    keys.add(keyd22);
    keys.add(keyd31);

    keys.add(keye11);
    keys.add(keye21);
    keys.add(keye31);

    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte)96);

    createKeys(ozoneBucket, keys);


    setKeyPrefix(createSnapshot().substring(1));
   
    // Delete the active fs so that we don't inadvertently read it
    deleteRootDir();
    // Root level listing keys
    Iterator<? extends OzoneKey> ozoneKeyIterator =
        ozoneBucket.listKeys(keyPrefix, null);
    verifyFullTreeStructure(ozoneKeyIterator);

    ozoneKeyIterator =
        ozoneBucket.listKeys(keyPrefix + "a/", null);
    verifyFullTreeStructure(ozoneKeyIterator);

    LinkedList<String> expectedKeys;

    // Intermediate level keyPrefix - 2nd level
    ozoneKeyIterator =
        ozoneBucket.listKeys(keyPrefix + "a///b2///", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d3/d31.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Intermediate level keyPrefix - 3rd level
    ozoneKeyIterator =
        ozoneBucket.listKeys(keyPrefix + "a/b2/d1", null);
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary of a level
    ozoneKeyIterator =
      ozoneBucket.listKeys(keyPrefix + "a/b2/d2", keyPrefix + "a/b2/d2/d21.tx");
    expectedKeys = new LinkedList<>();
    expectedKeys.add("a/b2/d2/d22.tx");
    checkKeyList(ozoneKeyIterator, expectedKeys);

    // Boundary case - last node in the depth-first-traversal
    ozoneKeyIterator =
      ozoneBucket.listKeys(keyPrefix + "a/b3/e3", keyPrefix + "a/b3/e3/e31.tx");
    expectedKeys = new LinkedList<>();
    checkKeyList(ozoneKeyIterator, expectedKeys);
  }

  private void verifyFullTreeStructure(Iterator<? extends OzoneKey> keyItr) {
    LinkedList<String> expectedKeys = new LinkedList<>();
    expectedKeys.add("a/");
    expectedKeys.add("a/b1/");
    expectedKeys.add("a/b1/c1/");
    expectedKeys.add("a/b1/c1/c1.tx");
    expectedKeys.add("a/b1/c2/");
    expectedKeys.add("a/b1/c2/c2.tx");
    expectedKeys.add("a/b2/");
    expectedKeys.add("a/b2/d1/");
    expectedKeys.add("a/b2/d1/d11.tx");
    expectedKeys.add("a/b2/d2/");
    expectedKeys.add("a/b2/d2/d21.tx");
    expectedKeys.add("a/b2/d2/d22.tx");
    expectedKeys.add("a/b2/d3/");
    expectedKeys.add("a/b2/d3/d31.tx");
    expectedKeys.add("a/b3/");
    expectedKeys.add("a/b3/e1/");
    expectedKeys.add("a/b3/e1/e11.tx");
    expectedKeys.add("a/b3/e2/");
    expectedKeys.add("a/b3/e2/e21.tx");
    expectedKeys.add("a/b3/e3/");
    expectedKeys.add("a/b3/e3/e31.tx");
    checkKeyList(keyItr, expectedKeys);
  }

  private void checkKeyList(Iterator<? extends OzoneKey > ozoneKeyIterator,
      List<String> keys) {

    LinkedList<String> outputKeys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      String keyName = ozoneKey.getName();
      if (keyName.startsWith(keyPrefix)) {
        keyName = keyName.substring(keyPrefix.length());
      }
      outputKeys.add(keyName);
    }
    Assert.assertEquals(keys, outputKeys);
  }

  private void createKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
    }
  }

  private void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input) throws Exception {

    OzoneOutputStream ozoneOutputStream =
            ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    String inputString = new String(input, StandardCharsets.UTF_8);
    Assert.assertEquals(inputString, new String(read, StandardCharsets.UTF_8));

    // Read using filesystem.
    String rootPath = String.format("%s://%s.%s/", OZONE_URI_SCHEME,
            bucketName, volumeName, StandardCharsets.UTF_8);
    OzoneFileSystem o3fsNew = (OzoneFileSystem) FileSystem
        .get(new URI(rootPath), conf);
    FSDataInputStream fsDataInputStream = o3fsNew.open(new Path(key));
    read = new byte[length];
    fsDataInputStream.read(read, 0, length);
    ozoneInputStream.close();

    Assert.assertEquals(inputString, new String(read, StandardCharsets.UTF_8));
  }

  private static void setKeyPrefix(String s) {
    keyPrefix = s;
  }

  @Test
  // based on TestOzoneFileSystem:testListStatus
  public void testListStatus() throws Exception {
    Path root = new Path("/");
    Path parent = new Path(root, "/testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    String snapshotKeyPrefix = createSnapshot();
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    Assert.assertEquals("Should be empty", 0, fileStatuses.length);

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    snapshotKeyPrefix = createSnapshot();
    snapshotRoot = new Path(snapshotKeyPrefix + root);
    Path snapshotParent = new Path(snapshotKeyPrefix + parent);
    fileStatuses = o3fs.listStatus(snapshotRoot);
    Assert.assertEquals("Should have created parent",
            1, fileStatuses.length);
    Assert.assertEquals("Parent path doesn't match",
            fileStatuses[0].getPath().toUri().getPath(),
            snapshotParent.toString());

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = o3fs.listStatus(snapshotParent);
    assertEquals("FileStatus did not return all children of the directory",
        2, fileStatuses.length);

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    snapshotKeyPrefix = createSnapshot();
    snapshotParent = new Path(snapshotKeyPrefix + parent);
    deleteRootDir();
    fileStatuses = o3fs.listStatus(snapshotParent);
    assertEquals("FileStatus did not return all children of the directory",
        3, fileStatuses.length);
  }

  @Test
  // based on TestOzoneFileSystem:testListStatusWithIntermediateDir
  public void testListStatusWithIntermediateDir() throws Exception {
    String keyName = "object-dir/object-name";
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setLocationInfoList(new ArrayList<>())
        .build();

    OpenKeySession session = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, session.getId());

    Path parent = new Path("/");

    // Wait until the filestatus is updated
    if (!enabledFileSystemPaths) {
      GenericTestUtils.waitFor(() -> {
        try {
          return fs.listStatus(parent).length != 0;
        } catch (IOException e) {
          LOG.error("listStatus() Failed", e);
          Assert.fail("listStatus() Failed");
          return false;
        }
      }, 1000, 120000);
    }

    String snapshotKeyPrefix = createSnapshot();
    deleteRootDir();
    Path snapshotParent = new Path(snapshotKeyPrefix + parent);
    FileStatus[] fileStatuses = fs.listStatus(snapshotParent);

    // the number of immediate children of root is 1
    Assert.assertEquals(1, fileStatuses.length);
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  // based on TestOzoneFileSystem:testListStatusOnRoot
  public void testListStatusOnRoot() throws Exception {
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    String snapshotKeyPrefix = createSnapshot();
    deleteRootDir();
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    assertEquals("FileStatus should return only the immediate children",
        2, fileStatuses.length);

    // Verify that dir12 is not included in the result of the listStatus on root
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertNotEquals(fileStatus1, dir12.toString());
    assertNotEquals(fileStatus2, dir12.toString());
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  // based on TestOzoneFileSystem:testListStatusOnLargeDirectory
  public void testListStatusOnLargeDirectory() throws Exception {
    Path root = new Path("/");
    deleteRootDir(); // cleanup
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for (int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
    }

    String snapshotKeyPrefix = createSnapshot();
    deleteRootDir();
    Path snapshotRoot = new Path(snapshotKeyPrefix + root);
    FileStatus[] fileStatuses = o3fs.listStatus(snapshotRoot);
    // Added logs for debugging failures, to check any sub-path mismatches.
    Set<String> actualPaths = new TreeSet<>();
    ArrayList<String> actualPathList = new ArrayList<>();
    if (numDirs != fileStatuses.length) {
      for (int i = 0; i < fileStatuses.length; i++) {
        boolean duplicate =
                actualPaths.add(fileStatuses[i].getPath().getName());
        if (!duplicate) {
          LOG.info("Duplicate path:{} in FileStatusList",
                  fileStatuses[i].getPath().getName());
        }
        actualPathList.add(fileStatuses[i].getPath().getName());
      }
      if (numDirs != actualPathList.size()) {
        LOG.info("actualPathsSize: {}", actualPaths.size());
        LOG.info("actualPathListSize: {}", actualPathList.size());
        actualPaths.removeAll(paths);
        actualPathList.removeAll(paths);
        LOG.info("actualPaths: {}", actualPaths);
        LOG.info("actualPathList: {}", actualPathList);
      }
    }
    assertEquals(
        "Total directories listed do not match the existing directories",
        numDirs, fileStatuses.length);

    for (int i = 0; i < numDirs; i++) {
      assertTrue(paths.contains(fileStatuses[i].getPath().getName()));
    }
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  @After
  public void deleteRootDir()
      throws IOException, InterruptedException, TimeoutException {
    Path root = new Path("/");
    FileStatus[] fileStatuses = fs.listStatus(root);

    if (fileStatuses == null) {
      return;
    }

    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }


    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(root).length == 0;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 120000);
  }

  private String createSnapshot()
      throws IOException, InterruptedException, TimeoutException {

    // create snapshot
    String snapshotName = UUID.randomUUID().toString();
    writeClient.createSnapshot(volumeName, bucketName, snapshotName);

    // wait till the snapshot directory exists
    SnapshotInfo snapshotInfo = ozoneManager.getMetadataManager()
        .getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName));
    String snapshotDirName = metaDir + OM_KEY_PREFIX +
        OM_SNAPSHOT_DIR + OM_KEY_PREFIX + OM_DB_NAME +
        snapshotInfo.getCheckpointDirName() + OM_KEY_PREFIX + "CURRENT";
    GenericTestUtils.waitFor(() -> new File(snapshotDirName).exists(),
        1000, 120000);

    return OM_KEY_PREFIX + OmSnapshotManager.getSnapshotPrefix(snapshotName);
  }
}
