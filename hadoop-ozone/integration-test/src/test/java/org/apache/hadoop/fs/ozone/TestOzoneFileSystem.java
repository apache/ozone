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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.TrashPolicyOzone;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.apache.ozone.test.tag.Flaky;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_ACLS;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_CHECKSUMS;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
  public Timeout timeout = Timeout.seconds(600);

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
    } catch (IOException | InterruptedException ex) {
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

  @Test
  public void testCreateFileShouldCheckExistenceOfDirWithSameName()
      throws Exception {
    /*
     * Op 1. create file -> /d1/d2/d3/d4/key2
     * Op 2. create dir -> /d1/d2/d3/d4/key2
     *
     * Reverse of the above steps
     * Op 2. create dir -> /d1/d2/d3/d4/key3
     * Op 1. create file -> /d1/d2/d3/d4/key3
     *
     * Op 3. create file -> /d1/d2/d3 (d3 as a file inside /d1/d2)
     */

    Path parent = new Path("/d1/d2/d3/d4/");
    Path file1 = new Path(parent, "key1");
    try (FSDataOutputStream outputStream = fs.create(file1, false)) {
      assertNotNull("Should be able to create file", outputStream);
    }

    Path dir1 = new Path("/d1/d2/d3/d4/key2");
    fs.mkdirs(dir1);
    try (FSDataOutputStream outputStream1 = fs.create(dir1, false)) {
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae) {
      // ignore as its expected
    }

    Path file2 = new Path("/d1/d2/d3/d4/key3");
    try (FSDataOutputStream outputStream2 = fs.create(file2, false)) {
      assertNotNull("Should be able to create file", outputStream2);
    }
    try {
      fs.mkdirs(file2);
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae) {
      // ignore as its expected
    }

    // Op 3. create file -> /d1/d2/d3 (d3 as a file inside /d1/d2)
    Path file3 = new Path("/d1/d2/d3");
    try (FSDataOutputStream outputStream3 = fs.create(file3, false)) {
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae) {
      // ignore as its expected
    }

    // Directory
    FileStatus fileStatus = fs.getFileStatus(parent);
    assertEquals("FileStatus did not return the directory",
            "/d1/d2/d3/d4", fileStatus.getPath().toUri().getPath());
    assertTrue("FileStatus did not return the directory",
            fileStatus.isDirectory());

    // invalid sub directory
    try {
      fs.getFileStatus(new Path("/d1/d2/d3/d4/key3/invalid"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
    // invalid file name
    try {
      fs.getFileStatus(new Path("/d1/d2/d3/d4/invalidkey"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has roughly the semantics of Unix @{code mkdir -p}.
   * {@link FileSystem#mkdirs(Path)}
   */
  @Test
  public void testMakeDirsWithAnExistingDirectoryPath() throws Exception {
    /*
     * Op 1. create file -> /d1/d2/d3/d4/k1 (d3 is a sub-dir inside /d1/d2)
     * Op 2. create dir -> /d1/d2
     */
    Path parent = new Path("/d1/d2/d3/d4/");
    Path file1 = new Path(parent, "key1");
    try (FSDataOutputStream outputStream = fs.create(file1, false)) {
      assertNotNull("Should be able to create file", outputStream);
    }

    Path subdir = new Path("/d1/d2/");
    boolean status = fs.mkdirs(subdir);
    assertTrue("Shouldn't send error if dir exists", status);
  }

  @Test
  public void testMakeDirsWithAnFakeDirectory() throws Exception {
    /*
     * Op 1. commit a key -> "dir1/dir2/key1"
     * Op 2. create dir -> "dir1/testDir", the dir1 is a fake dir,
     *  "dir1/testDir" can be created normal
     */

    String fakeGrandpaKey = "dir1";
    String fakeParentKey = fakeGrandpaKey + "/dir2";
    String fullKeyName = fakeParentKey + "/key1";
    TestDataUtil.createKey(ozoneBucket, fullKeyName, "");

    // /dir1/dir2 should not exist
    assertFalse(fs.exists(new Path(fakeParentKey)));

    // /dir1/dir2/key2 should be created because has a fake parent directory
    Path subdir = new Path(fakeParentKey, "key2");
    assertTrue(fs.mkdirs(subdir));
    // the intermediate directories /dir1 and /dir1/dir2 will be created too
    assertTrue(fs.exists(new Path(fakeGrandpaKey)));
    assertTrue(fs.exists(new Path(fakeParentKey)));
  }

  @Test
  public void testCreateWithInvalidPaths() throws Exception {
    // Test for path with ..
    Path parent = new Path("../../../../../d1/d2/");
    Path file1 = new Path(parent, "key1");
    checkInvalidPath(file1);

    // Test for path with :
    file1 = new Path("/:/:");
    checkInvalidPath(file1);

    // Test for path with scheme and authority.
    file1 = new Path(fs.getUri() + "/:/:");
    checkInvalidPath(file1);
  }

  private void checkInvalidPath(Path path) {
    InvalidPathException pathException = assertThrows(
        InvalidPathException.class, () -> fs.create(path, false)
    );
    assertTrue(pathException.getMessage().contains("Invalid path Name"));
  }

  @Test
  public void testOzoneFsServiceLoader() throws IOException {
    assertEquals(
        FileSystem.getFileSystemClass(OzoneConsts.OZONE_URI_SCHEME, null),
        OzoneFileSystem.class);
  }

  @Test
  public void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path("/testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    OzoneKeyDetails key = getKey(child, false);
    assertEquals(key.getName(), o3fs.pathToKey(child));

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // List status on the parent should show the child file
    assertEquals("List status of parent should include the 1 child file", 1L,
        fs.listStatus(parent).length);
    assertTrue("Parent directory does not appear to be a directory",
        fs.getFileStatus(parent).isDirectory());
  }

  @Test
  public void testDeleteCreatesFakeParentDir() throws Exception {
    Path grandparent = new Path("/testDeleteCreatesFakeParentDir");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    // Verify that parent dir key does not exist
    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // Delete the child key
    Assert.assertTrue(fs.delete(child, false));

    // Deleting the only child should create the parent dir key if it does
    // not exist
    FileStatus fileStatus = o3fs.getFileStatus(parent);
    Assert.assertTrue(fileStatus.isDirectory());
    assertEquals(parent.toString(), fileStatus.getPath().toUri().getPath());

    // Recursive delete with DeleteIterator
    Assert.assertTrue(fs.delete(grandparent, true));
  }

  @Test
  public void testRecursiveDelete() throws Exception {
    Path grandparent = new Path("/gdir1");

    for (int i = 1; i <= 10; i++) {
      Path parent = new Path(grandparent, "pdir" + i);
      Path child = new Path(parent, "child");
      ContractTestUtils.touch(fs, child);
    }

    // delete a dir with sub-file
    try {
      FileStatus[] parents = fs.listStatus(grandparent);
      Assert.assertTrue(parents.length > 0);
      fs.delete(parents[0].getPath(), false);
      Assert.fail("Must throw exception as dir is not empty!");
    } catch (PathIsNotEmptyDirectoryException pde) {
      // expected
    }

    // delete a dir with sub-file
    try {
      fs.delete(grandparent, false);
      Assert.fail("Must throw exception as dir is not empty!");
    } catch (PathIsNotEmptyDirectoryException pde) {
      // expected
    }

    // Delete the grandparent, which should delete all keys.
    fs.delete(grandparent, true);

    checkPath(grandparent);

    for (int i = 1; i <= 10; i++) {
      Path parent = new Path(grandparent, "dir" + i);
      Path child = new Path(parent, "child");
      checkPath(parent);
      checkPath(child);
    }


    Path level0 = new Path("/level0");

    for (int i = 1; i <= 3; i++) {
      Path level1 = new Path(level0, "level" + i);
      Path level2 = new Path(level1, "level" + i);
      Path level1File = new Path(level1, "file1");
      Path level2File = new Path(level2, "file1");
      ContractTestUtils.touch(fs, level1File);
      ContractTestUtils.touch(fs, level2File);
    }

    // Delete at sub directory level.
    for (int i = 1; i <= 3; i++) {
      Path level1 = new Path(level0, "level" + i);
      Path level2 = new Path(level1, "level" + i);
      fs.delete(level2, true);
      fs.delete(level1, true);
    }


    // Delete level0 finally.
    fs.delete(level0, true);

    // Check if it exists or not.
    checkPath(level0);

    for (int i = 1; i <= 3; i++) {
      Path level1 = new Path(level0, "level" + i);
      Path level2 = new Path(level1, "level" + i);
      Path level1File = new Path(level1, "file1");
      Path level2File = new Path(level2, "file1");
      checkPath(level1);
      checkPath(level2);
      checkPath(level1File);
      checkPath(level2File);
    }
  }

  private void checkPath(Path path) {
    try {
      fs.getFileStatus(path);
      fail("testRecursiveDelete failed");
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof FileNotFoundException);
      Assert.assertTrue(ex.getMessage().contains("No such file or directory"));
    }
  }

  @Test
  public void testFileDelete() throws Exception {
    Path grandparent = new Path("/testBatchDelete");
    Path parent = new Path(grandparent, "parent");
    Path childFolder = new Path(parent, "childFolder");
    // BatchSize is 5, so we're going to set a number that's not a
    // multiple of 5. In order to test the final number of keys less than
    // batchSize can also be deleted.
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      Path childFolderFile = new Path(childFolder, "child" + i);
      ContractTestUtils.touch(fs, childFile);
      ContractTestUtils.touch(fs, childFolderFile);
    }

    assertEquals(1, fs.listStatus(grandparent).length);
    assertEquals(9, fs.listStatus(parent).length);
    assertEquals(8, fs.listStatus(childFolder).length);

    assertTrue(fs.delete(grandparent, true));
    assertFalse(o3fs.exists(grandparent));
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      // Make sure all keys under testBatchDelete/parent should be deleted
      assertFalse(o3fs.exists(childFile));

      // Test to recursively delete child folder, make sure all keys under
      // testBatchDelete/parent/childFolder should be deleted.
      Path childFolderFile = new Path(childFolder, "child" + i);
      assertFalse(o3fs.exists(childFolderFile));
    }
    // Will get: WARN  ozone.BasicOzoneFileSystem delete: Path does not exist.
    // This will return false.
    assertFalse(fs.delete(parent, true));
  }

  @Test
  public void testListStatus() throws Exception {
    Path parent = new Path(ROOT, "/testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    FileStatus[] fileStatuses = o3fs.listStatus(ROOT, EXCLUDE_TRASH);
    Assert.assertEquals("Should be empty", 0, fileStatuses.length);

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = o3fs.listStatus(ROOT, EXCLUDE_TRASH);
    Assert.assertEquals("Should have created parent",
            1, fileStatuses.length);
    Assert.assertEquals("Parent path doesn't match",
            fileStatuses[0].getPath().toUri().getPath(), parent.toString());

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = o3fs.listStatus(parent);
    assertEquals("FileStatus did not return all children of the directory",
        2, fileStatuses.length);

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    fileStatuses = o3fs.listStatus(parent);
    assertEquals("FileStatus did not return all children of the directory",
        3, fileStatuses.length);
  }

  @Test
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

    // Wait until the filestatus is updated
    if (!enabledFileSystemPaths) {
      GenericTestUtils.waitFor(() -> {
        try {
          return fs.listStatus(ROOT, EXCLUDE_TRASH).length != 0;
        } catch (IOException e) {
          LOG.error("listStatus() Failed", e);
          Assert.fail("listStatus() Failed");
          return false;
        }
      }, 1000, 120000);
    }

    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);

    // the number of immediate children of root is 1
    Assert.assertEquals(Arrays.toString(fileStatuses), 1, fileStatuses.length);
    writeClient.deleteKey(keyArgs);
  }

  @Test
  public void testListStatusWithIntermediateDirWithECEnabled()
          throws Exception {
    String keyName = "object-dir/object-name1";
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(new ECReplicationConfig(3, 2))
            .setLocationInfoList(new ArrayList<>())
            .build();
    OpenKeySession session = writeClient.openKey(keyArgs);
    writeClient.commitKey(keyArgs, session.getId());
    // Wait until the filestatus is updated
    if (!enabledFileSystemPaths) {
      GenericTestUtils.waitFor(() -> {
        try {
          return fs.listStatus(ROOT, EXCLUDE_TRASH).length != 0;
        } catch (IOException e) {
          LOG.error("listStatus() Failed", e);
          Assert.fail("listStatus() Failed");
          return false;
        }
      }, 1000, 120000);
    }
    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    // the number of immediate children of root is 1
    Assert.assertEquals(1, fileStatuses.length);
    Assert.assertEquals(fileStatuses[0].isErasureCoded(),
            !bucketLayout.isFileSystemOptimized());
    fileStatuses = fs.listStatus(new Path(
            fileStatuses[0].getPath().toString() + "/object-name1"));
    Assert.assertEquals(1, fileStatuses.length);
    Assert.assertTrue(fileStatuses[0].isErasureCoded());
    writeClient.deleteKey(keyArgs);
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  public void testListStatusOnRoot() throws Exception {
    Path dir1 = new Path(ROOT, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(ROOT, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    FileStatus[] fileStatuses = o3fs.listStatus(ROOT, EXCLUDE_TRASH);
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
  public void testListStatusOnLargeDirectory() throws Exception {
    deleteRootDir(); // cleanup
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for (int i = 0; i < numDirs; i++) {
      Path p = new Path(ROOT, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
    }

    FileStatus[] fileStatuses = o3fs.listStatus(ROOT, EXCLUDE_TRASH);
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

  @Test
  public void testListStatusOnKeyNameContainDelimiter() throws Exception {
    /*
    * op1: create a key -> "dir1/dir2/key1"
    * op2: `ls /` child dir "/dir1/" will be return
    * op2: `ls /dir1` child dir "/dir1/dir2/" will be return
    * op3: `ls /dir1/dir2` file "/dir1/dir2/key" will be return
    *
    * the "/dir1", "/dir1/dir2/" are fake directory
    * */
    String keyName = "dir1/dir2/key1";
    TestDataUtil.createKey(ozoneBucket, keyName, "");
    FileStatus[] fileStatuses;

    fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    assertEquals(1, fileStatuses.length);
    assertEquals("/dir1", fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());

    fileStatuses = fs.listStatus(new Path("/dir1"));
    assertEquals(1, fileStatuses.length);
    assertEquals("/dir1/dir2", fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());

    fileStatuses = fs.listStatus(new Path("/dir1/dir2"));
    assertEquals(1, fileStatuses.length);
    assertEquals("/dir1/dir2/key1",
        fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isFile());
  }

  /**
   * Cleanup files and directories.
   *
   * @throws IOException DB failure
   */
  protected void deleteRootDir() throws IOException, InterruptedException {
    FileStatus[] fileStatuses = fs.listStatus(ROOT);

    if (fileStatuses == null) {
      return;
    }
    deleteRootRecursively(fileStatuses);
    fileStatuses = fs.listStatus(ROOT);
    if (fileStatuses != null) {
      for (FileStatus fileStatus : fileStatuses) {
        LOG.error("Unexpected file, should have been deleted: {}", fileStatus);
      }
      Assert.assertEquals("Delete root failed!", 0, fileStatuses.length);
    }
  }

  private static void deleteRootRecursively(FileStatus[] fileStatuses)
      throws IOException {
    for (FileStatus fStatus : fileStatuses) {
      fs.delete(fStatus.getPath(), true);
    }
  }

  /**
   * Tests listStatus on a path with subdirs.
   */
  @Test
  public void testListStatusOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatus on /dir1 should return all its immediate subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatus.
    Path dir1 = new Path("/dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path("/dir2");
    fs.mkdirs(dir111);
    fs.mkdirs(dir12);
    ContractTestUtils.touch(fs, file121);
    fs.mkdirs(dir2);

    FileStatus[] fileStatuses = o3fs.listStatus(dir1);
    assertEquals("FileStatus should return only the immediate children", 2,
        fileStatuses.length);

    // Verify that the two children of /dir1 returned by listStatus operation
    // are /dir1/dir11 and /dir1/dir12.
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertTrue(fileStatus1.equals(dir11.toString()) ||
        fileStatus1.equals(dir12.toString()));
    assertTrue(fileStatus2.equals(dir11.toString()) ||
        fileStatus2.equals(dir12.toString()));
  }

  /**
   * Tests listStatusIterator operation on root directory.
   */
  @Test
  public void testListStatusIteratorWithDir() throws Exception {
    Path parent = new Path(ROOT, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");
    try {
      // Iterator should have no items when dir is empty
      RemoteIterator<FileStatus> it = o3fs.listStatusIterator(ROOT);
      Assert.assertFalse(it.hasNext());

      ContractTestUtils.touch(fs, file1);
      ContractTestUtils.touch(fs, file2);
      // Iterator should have an item when dir is not empty
      it = o3fs.listStatusIterator(ROOT);
      while (it.hasNext()) {
        FileStatus fileStatus = it.next();
        Assert.assertNotNull(fileStatus);
        Assert.assertEquals("Parent path doesn't match",
            fileStatus.getPath().toUri().getPath(), parent.toString());
      }
      // Iterator on a directory should return all subdirs along with
      // files, even if there exists a file and sub-dir with the same name.
      it = o3fs.listStatusIterator(parent);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        Assert.assertNotNull(fileStatus);
      }
      Assert.assertEquals(
          "Iterator did not return all the file status",
          2, iCount);
      // Iterator should return file status for only the
      // immediate children of a directory.
      Path file3 = new Path(parent, "dir1/key3");
      Path file4 = new Path(parent, "dir1/key4");
      ContractTestUtils.touch(fs, file3);
      ContractTestUtils.touch(fs, file4);
      it = o3fs.listStatusIterator(parent);
      iCount = 0;

      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        Assert.assertNotNull(fileStatus);
      }
      Assert.assertEquals("Iterator did not return file status " +
          "of all the children of the directory", 3, iCount);

    } finally {
      // Cleanup
      fs.delete(parent, true);
    }
  }

  /**
   * Tests listStatusIterator operation on root directory.
   */
  @Test
  public void testListStatusIteratorOnRoot() throws Exception {
    Path dir1 = new Path(ROOT, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(ROOT, "dir2");
    try {
      fs.mkdirs(dir12);
      fs.mkdirs(dir2);

      // ListStatusIterator on root should return dir1
      // (even though /dir1 key does not exist)and dir2 only.
      // dir12 is not an immediate child of root and hence should not be listed.
      RemoteIterator<FileStatus> it = o3fs.listStatusIterator(ROOT);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        Assert.assertNotNull(fileStatus);
        // Verify that dir12 is not included in the result
        // of the listStatusIterator on root.
        assertNotEquals(fileStatus.getPath().toUri().getPath(),
            dir12.toString());
      }
      assertEquals("FileStatus should return only the immediate children",
          2, iCount);
    } finally {
      // Cleanup
      fs.delete(dir2, true);
      fs.delete(dir1, true);
    }
  }

  @Test
  public void testListStatusIteratorOnPageSize() throws Exception {
    OzoneFileSystemTests.listStatusIteratorOnPageSize(cluster.getConf(),
        "/" + volumeName + "/" + bucketName);
  }

  /**
   * Tests listStatus on a path with subdirs.
   */
  @Test
  public void testListStatusIteratorOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatusIterator on /dir1 should return file status for
    // all its immediate subdirs only which are /dir1/dir11 and
    // /dir1/dir12. Super child files/dirs (/dir1/dir12/file121
    // and /dir1/dir11/dir111) should not be returned by
    // listStatusIterator.
    Path dir1 = new Path("/dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path("/dir2");
    try {
      fs.mkdirs(dir111);
      fs.mkdirs(dir12);
      ContractTestUtils.touch(fs, file121);
      fs.mkdirs(dir2);

      RemoteIterator<FileStatus> it = o3fs.listStatusIterator(dir1);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        Assert.assertNotNull(fileStatus);
        assertTrue(fileStatus.getPath().toUri().getPath().
            equals(dir11.toString()) || fileStatus.getPath().toUri().getPath()
            .equals(dir12.toString()));
      }
      assertEquals("FileStatus should return only the immediate children", 2,
          iCount);
    } finally {
      // Cleanup
      fs.delete(dir2, true);
      fs.delete(dir1, true);
    }
  }

  @Test
  public void testSeekOnFileLength() throws IOException {
    Path file = new Path("/file");
    ContractTestUtils.createFile(fs, file, true, "a".getBytes(UTF_8));
    try (FSDataInputStream stream = fs.open(file)) {
      long fileLength = fs.getFileStatus(file).getLen();
      stream.seek(fileLength);
      assertEquals(-1, stream.read());
    }

    // non-existent file
    Path fileNotExists = new Path("/file_notexist");
    try {
      fs.open(fileNotExists);
      Assert.fail("Should throw FileNotFoundException as file doesn't exist!");
    } catch (FileNotFoundException fnfe) {
      Assert.assertTrue("Expected KEY_NOT_FOUND error",
              fnfe.getMessage().contains("KEY_NOT_FOUND"));
    }
  }

  @Test
  public void testAllocateMoreThanOneBlock() throws IOException {
    Path file = new Path("/file");
    String str = "TestOzoneFileSystem.testAllocateMoreThanOneBlock";
    byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
    long numBlockAllocationsOrg =
            cluster.getOzoneManager().getMetrics().getNumBlockAllocates();

    try (FSDataOutputStream out1 = fs.create(file, FsPermission.getDefault(),
            true, 8, (short) 3, 1, null)) {
      for (int i = 0; i < 100000; i++) {
        out1.write(strBytes);
      }
    }

    try (FSDataInputStream stream = fs.open(file)) {
      FileStatus fileStatus = fs.getFileStatus(file);
      long blkSize = fileStatus.getBlockSize();
      long fileLength = fileStatus.getLen();
      Assert.assertTrue("Block allocation should happen",
              fileLength > blkSize);

      long newNumBlockAllocations =
              cluster.getOzoneManager().getMetrics().getNumBlockAllocates();

      Assert.assertTrue("Block allocation should happen",
              (newNumBlockAllocations > numBlockAllocationsOrg));

      stream.seek(fileLength);
      assertEquals(-1, stream.read());
    }
  }

  public void testDeleteRoot() throws IOException {
    Path dir = new Path("/dir");
    fs.mkdirs(dir);
    assertFalse(fs.delete(ROOT, true));
    assertNotNull(fs.getFileStatus(dir));
  }

  @Test
  public void testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved()
      throws Exception {
    Path source = new Path("/source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path("/target");
    Path leafInTarget = new Path(target, "leaf");

    fs.mkdirs(source);
    fs.mkdirs(target);
    fs.mkdirs(leafInsideInterimPath);
    assertTrue(fs.rename(leafInsideInterimPath, leafInTarget));

    // after rename listStatus for interimPath should succeed and
    // interimPath should have no children
    FileStatus[] statuses = fs.listStatus(interimPath);
    assertNotNull("liststatus returns a null array", statuses);
    assertEquals("Statuses array is not empty", 0, statuses.length);
    FileStatus fileStatus = fs.getFileStatus(interimPath);
    assertEquals("FileStatus does not point to interimPath",
        interimPath.getName(), fileStatus.getPath().getName());
  }

  /**
   * Case-1) fromKeyName should exist, otw throws exception.
   */
  @Test
  public void testRenameWithNonExistentSource() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final String dir2 = root + "/dir2";
    final Path source = new Path(fs.getUri().toString() + dir1);
    final Path destin = new Path(fs.getUri().toString() + dir2);

    // creates destin
    fs.mkdirs(destin);
    LOG.info("Created destin dir: {}", destin);

    LOG.info("Rename op-> source:{} to destin:{}}", source, destin);
    assertFalse("Expected to fail rename as src doesn't exist",
            fs.rename(source, destin));
  }

  /**
   * Case-2) Cannot rename a directory to its own subdirectory.
   */
  @Test
  public void testRenameDirToItsOwnSubDir() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(fs.getUri().toString() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    fs.mkdirs(subDir1);
    LOG.info("Created dir1 {}", subDir1);

    final Path sourceRoot = new Path(fs.getUri().toString() + root);
    LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
    try {
      fs.rename(sourceRoot, subDir1);
      Assert.fail("Should throw exception : Cannot rename a directory to" +
              " its own subdirectory");
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  /**
   * Case-3) If src == destin then check source and destin of same type.
   */
  @Test
  public void testRenameSourceAndDestinAreSame() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2Path = new Path(fs.getUri().toString() + dir2);
    fs.mkdirs(dir2Path);

    // File rename
    Path file1 = new Path(fs.getUri().toString() + dir2 + "/file1");
    ContractTestUtils.touch(fs, file1);

    assertTrue(fs.rename(file1, file1));
    assertTrue(fs.rename(dir2Path, dir2Path));
  }

  /**
   * Case-4) Rename from /a, to /b.
   * <p>
   * Expected Result: After rename the directory structure will be /b/a.
   */
  @Test
  public void testRenameToExistingDir() throws Exception {
    // created /a
    final Path aSourcePath = new Path(fs.getUri().toString() + "/a");
    fs.mkdirs(aSourcePath);

    // created /b
    final Path bDestinPath = new Path(fs.getUri().toString() + "/b");
    fs.mkdirs(bDestinPath);

    // Add a sub-directory '/a/c' to '/a'. This is to verify that after
    // rename sub-directory also be moved.
    final Path acPath = new Path(fs.getUri().toString() + "/a/c");
    fs.mkdirs(acPath);

    // Rename from /a to /b.
    assertTrue("Rename failed", fs.rename(aSourcePath, bDestinPath));

    final Path baPath = new Path(fs.getUri().toString() + "/b/a");
    final Path bacPath = new Path(fs.getUri().toString() + "/b/a/c");
    assertTrue("Rename failed", fs.exists(baPath));
    assertTrue("Rename failed", fs.exists(bacPath));
  }

  /**
   * Case-5) If new destin '/dst/source' exists then throws exception.
   * If destination is a directory then rename source as sub-path of it.
   * <p>
   * For example: rename /a to /b will lead to /b/a. This new path should
   * not exist.
   */
  @Test
  public void testRenameToNewSubDirShouldNotExist() throws Exception {
    // Case-5.a) Rename directory from /a to /b.
    // created /a
    final Path aSourcePath = new Path(fs.getUri().toString() + "/a");
    fs.mkdirs(aSourcePath);

    // created /b
    final Path bDestinPath = new Path(fs.getUri().toString() + "/b");
    fs.mkdirs(bDestinPath);

    // Add a sub-directory '/b/a' to '/b'. This is to verify that rename
    // throws exception as new destin /b/a already exists.
    final Path baPath = new Path(fs.getUri().toString() + "/b/a/c");
    fs.mkdirs(baPath);

    Assert.assertFalse("New destin sub-path /b/a already exists",
            fs.rename(aSourcePath, bDestinPath));

    // Case-5.b) Rename file from /a/b/c/file1 to /a.
    // Should be failed since /a/file1 exists.
    final Path abcPath = new Path(fs.getUri().toString() + "/a/b/c");
    fs.mkdirs(abcPath);
    Path abcFile1 = new Path(abcPath, "/file1");
    ContractTestUtils.touch(fs, abcFile1);

    final Path aFile1 = new Path(fs.getUri().toString() + "/a/file1");
    ContractTestUtils.touch(fs, aFile1);

    final Path aDestinPath = new Path(fs.getUri().toString() + "/a");

    Assert.assertFalse("New destin sub-path /b/a already exists",
            fs.rename(abcFile1, aDestinPath));
  }

  /**
   * Case-6) Rename directory to an existed file, should be failed.
   */
  @Test
  public void testRenameDirToFile() throws Exception {
    final String root = "/root";
    Path rootPath = new Path(fs.getUri().toString() + root);
    fs.mkdirs(rootPath);

    Path file1Destin = new Path(fs.getUri().toString() + root + "/file1");
    ContractTestUtils.touch(fs, file1Destin);
    Path abcRootPath = new Path(fs.getUri().toString() + "/a/b/c");
    fs.mkdirs(abcRootPath);
    Assert.assertFalse("key already exists /root_dir/file1",
            fs.rename(abcRootPath, file1Destin));
  }

  /**
   * Rename file to a non-existent destin file.
   */
  @Test
  public void testRenameFile() throws Exception {
    final String root = "/root";
    Path rootPath = new Path(fs.getUri().toString() + root);
    fs.mkdirs(rootPath);

    Path file1Source = new Path(fs.getUri().toString() + root
            + "/file1_Copy");
    ContractTestUtils.touch(fs, file1Source);
    Path file1Destin = new Path(fs.getUri().toString() + root + "/file1");
    assertTrue("Renamed failed", fs.rename(file1Source, file1Destin));
    assertTrue("Renamed failed: /root/file1", fs.exists(file1Destin));

    /**
     * Reading several times, this is to verify that OmKeyInfo#keyName cached
     * entry is not modified. While reading back, OmKeyInfo#keyName will be
     * prepared and assigned to fullkeyPath name.
     */
    for (int i = 0; i < 10; i++) {
      FileStatus[] fStatus = fs.listStatus(rootPath);
      assertEquals("Renamed failed", 1, fStatus.length);
      assertEquals("Wrong path name!", file1Destin, fStatus[0].getPath());
    }
  }

  /**
   * Rename file to an existed directory.
   */
  @Test
  public void testRenameFileToDir() throws Exception {
    final String root = "/root";
    Path rootPath = new Path(fs.getUri().toString() + root);
    fs.mkdirs(rootPath);

    Path file1Destin = new Path(fs.getUri().toString() + root + "/file1");
    ContractTestUtils.touch(fs, file1Destin);
    Path abcRootPath = new Path(fs.getUri().toString() + "/a/b/c");
    fs.mkdirs(abcRootPath);
    assertTrue("Renamed failed", fs.rename(file1Destin, abcRootPath));
    assertTrue("Renamed filed: /a/b/c/file1", fs.exists(new Path(abcRootPath,
            "file1")));
  }

  @Test
  public void testRenameContainDelimiterFile() throws Exception {
    String fakeGrandpaKey = "dir1";
    String fakeParentKey = fakeGrandpaKey + "/dir2";
    String sourceKeyName = fakeParentKey + "/key1";
    String targetKeyName = fakeParentKey +  "/key2";
    TestDataUtil.createKey(ozoneBucket, sourceKeyName, "");

    Path sourcePath = new Path(fs.getUri().toString() + "/" + sourceKeyName);
    Path targetPath = new Path(fs.getUri().toString() + "/" + targetKeyName);
    assertTrue(fs.rename(sourcePath, targetPath));
    assertFalse(fs.exists(sourcePath));
    assertTrue(fs.exists(targetPath));
    // intermediate directories will not be created
    assertFalse(fs.exists(new Path(fakeGrandpaKey)));
    assertFalse(fs.exists(new Path(fakeParentKey)));
  }


  /**
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Test
  public void testRenameDestinationParentDoesntExist() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(fs.getUri().toString() + dir2);
    fs.mkdirs(dir2SourcePath);

    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(fs.getUri().toString() + root + "/b/c");
    try {
      fs.rename(dir2SourcePath, destinPath);
      Assert.fail("Should fail as parent of dst does not exist!");
    } catch (FileNotFoundException fnfe) {
      // expected
    }

    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(fs.getUri().toString() + root + "/file1");
    ContractTestUtils.touch(fs, filePath);

    Path newDestinPath = new Path(filePath, "c");
    try {
      fs.rename(dir2SourcePath, newDestinPath);
      Assert.fail("Should fail as parent of dst is a file!");
    } catch (IOException ioe) {
      // expected
    }
  }

  /**
   * Rename to the source's parent directory, it will succeed.
   * 1. Rename from /root_dir/dir1/dir2 to /root_dir.
   * Expected result : /root_dir/dir2
   * <p>
   * 2. Rename from /root_dir/dir1/file1 to /root_dir.
   * Expected result : /root_dir/file1.
   */
  @Test
  public void testRenameToParentDir() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(fs.getUri().toString() + dir2);
    fs.mkdirs(dir2SourcePath);
    final Path destRootPath = new Path(fs.getUri().toString() + root);

    Path file1Source = new Path(fs.getUri().toString() + dir1 + "/file2");
    ContractTestUtils.touch(fs, file1Source);

    // rename source directory to its parent directory(destination).
    assertTrue("Rename failed", fs.rename(dir2SourcePath, destRootPath));
    final Path expectedPathAfterRename =
            new Path(fs.getUri().toString() + root + "/dir2");
    assertTrue("Rename failed",
            fs.exists(expectedPathAfterRename));

    // rename source file to its parent directory(destination).
    assertTrue("Rename failed", fs.rename(file1Source, destRootPath));
    final Path expectedFilePathAfterRename =
            new Path(fs.getUri().toString() + root + "/file2");
    assertTrue("Rename failed",
            fs.exists(expectedFilePathAfterRename));
  }

  @Test
  public void testRenameDir() throws Exception {
    final String dir = "/root_dir/dir1";
    final Path source = new Path(fs.getUri().toString() + dir);
    final Path dest = new Path(source.toString() + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    fs.mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    fs.rename(source, dest);
    assertTrue("Directory rename failed", fs.exists(dest));
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue("Keys under the renamed directory not renamed",
        fs.exists(new Path(dest, "sub_dir1")));

    // Test if one path belongs to other FileSystem.
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> fs.rename(new Path(fs.getUri().toString() + "fake" + dir), dest));
    assertTrue(exception.getMessage().contains("Wrong FS"));
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

  @Test
  public void testGetDirectoryModificationTime()
      throws IOException, InterruptedException {
    Path mdir1 = new Path("/mdir1");
    Path mdir11 = new Path(mdir1, "mdir11");
    Path mdir111 = new Path(mdir11, "mdir111");
    fs.mkdirs(mdir111);

    // Case 1: Dir key exist on server
    FileStatus[] fileStatuses = o3fs.listStatus(mdir11);
    // Above listStatus result should only have one entry: mdir111
    assertEquals(1, fileStatuses.length);
    assertEquals(mdir111.toString(),
        fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());
    // The dir key is actually created on server,
    // so modification time should always be the same value.
    long modificationTime = fileStatuses[0].getModificationTime();
    // Check modification time in a small loop, it should always be the same
    for (int i = 0; i < 5; i++) {
      Thread.sleep(10);
      fileStatuses = o3fs.listStatus(mdir11);
      assertEquals(modificationTime, fileStatuses[0].getModificationTime());
    }

    // Case 2: Dir key doesn't exist on server
    fileStatuses = o3fs.listStatus(mdir1);
    // Above listStatus result should only have one entry: mdir11
    assertEquals(1, fileStatuses.length);
    assertEquals(mdir11.toString(),
        fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());
    // Since the dir key doesn't exist on server, the modification time is
    // set to current time upon every listStatus request.
    modificationTime = fileStatuses[0].getModificationTime();
    // Check modification time in a small loop, it should be slightly larger
    // each time
    for (int i = 0; i < 5; i++) {
      Thread.sleep(10);
      fileStatuses = o3fs.listStatus(mdir1);
      assertTrue(modificationTime <= fileStatuses[0].getModificationTime());
    }
  }

  @Test
  public void testGetTrashRoot() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    // Input path doesn't matter, o3fs.getTrashRoot() only cares about username
    Path inPath1 = new Path("o3fs://bucket2.volume1/path/to/key");
    // Test with current user
    Path outPath1 = o3fs.getTrashRoot(inPath1);
    Path expectedOutPath1 = o3fs.makeQualified(new Path(TRASH_ROOT, username));
    Assert.assertEquals(expectedOutPath1, outPath1);
  }

  @Test
  public void testCreateKeyShouldUseRefreshedBucketReplicationConfig()
      throws IOException {
    OzoneBucket bucket =
        TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    final TestClock testClock = new TestClock(Instant.now(), ZoneOffset.UTC);

    String rootPath = String
        .format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME, bucket.getName(),
            bucket.getVolumeName());

    // Set the fs.defaultFS and start the filesystem
    Configuration conf = new OzoneConfiguration(cluster.getConf());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    OzoneFileSystem o3FS = (OzoneFileSystem) FileSystem.get(conf);

    //Let's reset the clock to control the time.
    ((BasicOzoneClientAdapterImpl) (o3FS.getAdapter())).setClock(testClock);

    createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key"),
        ReplicationType.RATIS);

    bucket.setReplicationConfig(new ECReplicationConfig("rs-3-2-1024k"));

    //After changing the bucket policy, it should create ec key, but o3fs will
    // refresh after some time. So, it will be sill old type.
    createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key1"),
        ReplicationType.RATIS);

    testClock.fastForward(300 * 1000 + 1);

    //After client bucket refresh time, it should create new type what is
    // available on bucket at that moment.
    createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key2"),
        ReplicationType.EC);

    // Rechecking the same steps with changing to Ratis again to check the
    // behavior is consistent.
    bucket.setReplicationConfig(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key3"),
        ReplicationType.EC);

    testClock.fastForward(300 * 1000 + 1);

    createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key4"),
        ReplicationType.RATIS);
  }

  private void createKeyAndAssertKeyType(OzoneBucket bucket,
      OzoneFileSystem o3FS, Path keyPath, ReplicationType expectedType)
      throws IOException {
    o3FS.createFile(keyPath).build().close();
    Assert.assertEquals(expectedType.name(),
        bucket.getKey(o3FS.pathToKey(keyPath)).getReplicationConfig()
            .getReplicationType().name());
  }

  @Test
  public void testGetTrashRoots() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userTrash = new Path(TRASH_ROOT, username);

    Collection<FileStatus> res = o3fs.getTrashRoots(false);
    Assert.assertEquals(0, res.size());

    fs.mkdirs(userTrash);
    res = o3fs.getTrashRoots(false);
    Assert.assertEquals(1, res.size());
    res.forEach(e -> Assert.assertEquals(
        userTrash.toString(), e.getPath().toUri().getPath()));
    // Only have one user trash for now
    res = o3fs.getTrashRoots(true);
    Assert.assertEquals(1, res.size());

    // Create a few more random user trash dir
    for (int i = 1; i <= 5; i++) {
      Path moreUserTrash = new Path(TRASH_ROOT, "trashuser" + i);
      fs.mkdirs(moreUserTrash);
    }

    // And create a file, which should be ignored
    fs.create(new Path(TRASH_ROOT, "trashuser99"));

    // allUsers = false should still return current user trash
    res = o3fs.getTrashRoots(false);
    Assert.assertEquals(1, res.size());
    res.forEach(e -> Assert.assertEquals(
        userTrash.toString(), e.getPath().toUri().getPath()));
    // allUsers = true should return all user trash
    res = o3fs.getTrashRoots(true);
    Assert.assertEquals(6, res.size());
  }

  @Test
  public void testDeleteRootWithTrash() throws IOException {
    // Try to delete root
    Path root = new Path(OZONE_URI_DELIMITER);
    Assert.assertThrows(IOException.class, () -> trash.moveToTrash(root));
    // Also try with TrashPolicyDefault
    OzoneConfiguration conf2 = new OzoneConfiguration(cluster.getConf());
    conf2.setClass("fs.trash.classname", TrashPolicyDefault.class,
        TrashPolicy.class);
    Trash trashPolicyDefault = new Trash(conf2);
    Assert.assertThrows(IOException.class,
        () -> trashPolicyDefault.moveToTrash(root));
  }

  /**
   * 1.Move a Key to Trash
   * 2.Verify that the key gets deleted by the trash emptier.
   */
  @Test
  @Flaky("HDDS-6645")
  public void testTrash() throws Exception {
    String testKeyName = "testKey2";
    Path path = new Path(OZONE_URI_DELIMITER, testKeyName);
    ContractTestUtils.touch(fs, path);
    Assert.assertTrue(trash.getConf().getClass(
        "fs.trash.classname", TrashPolicy.class).
        isAssignableFrom(TrashPolicyOzone.class));
    assertEquals(TRASH_INTERVAL, trash.getConf().
        getFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, 0), 0);

    // Construct paths
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userTrash = new Path(TRASH_ROOT, username);
    Path userTrashCurrent = new Path(userTrash, "Current");
    Path trashPath = new Path(userTrashCurrent, testKeyName);
    Assert.assertFalse(o3fs.exists(userTrash));

    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(path);
    // Added this assertion here and will be tested as part of testTrash
    // test case which needs to be tested with separate mini cluster having
    // emptier thread started with close match of timings of relevant
    // assertion statements and corresponding trash and checkpoint interval.
    Assert.assertTrue(o3fs.exists(userTrash));
    Assert.assertTrue(o3fs.exists(userTrashCurrent));

    // Wait until the TrashEmptier purges the key
    GenericTestUtils.waitFor(() -> {
      try {
        return !o3fs.exists(trashPath);
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed");
        Assert.fail("Delete from Trash Failed");
        return false;
      }
    }, 100, 120000);

    // userTrash path will contain the checkpoint folder
    FileStatus[] statusList = fs.listStatus(userTrash);
    Assert.assertNotEquals(Arrays.toString(statusList), 0, statusList.length);

    // wait for deletion of checkpoint dir
    GenericTestUtils.waitFor(() -> {
      try {
        return o3fs.listStatus(userTrash).length == 0;
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed", e);
        Assert.fail("Delete from Trash Failed");
        return false;
      }
    }, 1000, 120000);
  }

  @Test
  public void testListStatusOnLargeDirectoryForACLCheck() throws Exception {
    String keyName = "dir1/dir2/testListStatusOnLargeDirectoryForACLCheck";
    Path root = new Path(OZONE_URI_DELIMITER, keyName);
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for (int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      getFs().mkdirs(p);
      paths.add(keyName + OM_KEY_PREFIX + p.getName());
    }

    // unknown keyname
    try {
      new OzonePrefixPathImpl(getVolumeName(), getBucketName(), "invalidKey",
          cluster.getOzoneManager().getKeyManager());
      Assert.fail("Non-existent key name!");
    } catch (OMException ome) {
      Assert.assertEquals(OMException.ResultCodes.KEY_NOT_FOUND,
          ome.getResult());
    }

    OzonePrefixPathImpl ozonePrefixPath =
        new OzonePrefixPathImpl(getVolumeName(), getBucketName(), keyName,
            cluster.getOzoneManager().getKeyManager());

    OzoneFileStatus status = ozonePrefixPath.getOzoneFileStatus();
    Assert.assertNotNull(status);
    Assert.assertEquals(keyName, status.getTrimmedName());
    Assert.assertTrue(status.isDirectory());

    Iterator<? extends OzoneFileStatus> pathItr =
        ozonePrefixPath.getChildren(keyName);
    Assert.assertTrue("Failed to list keyPath:" + keyName, pathItr.hasNext());

    Set<String> actualPaths = new TreeSet<>();
    while (pathItr.hasNext()) {
      String pathname = pathItr.next().getTrimmedName();
      actualPaths.add(pathname);

      // no subpaths, expected an empty list
      Iterator<? extends OzoneFileStatus> subPathItr =
          ozonePrefixPath.getChildren(pathname);
      Assert.assertNotNull(subPathItr);
      Assert.assertFalse("Failed to list keyPath: " + pathname,
          subPathItr.hasNext());
    }

    Assert.assertEquals("ListStatus failed", paths.size(),
        actualPaths.size());

    for (String pathname : actualPaths) {
      paths.remove(pathname);
    }
    Assert.assertTrue("ListStatus failed:" + paths, paths.isEmpty());
  }

  @Test
  public void testFileSystemDeclaresCapability() throws Throwable {
    Path root = new Path(OZONE_URI_DELIMITER);
    assertHasPathCapabilities(fs, root, FS_ACLS);
    assertHasPathCapabilities(fs, root, FS_CHECKSUMS);
  }

  @Test
  public void testSetTimes() throws Exception {
    // Create a file
    String testKeyName = "testKey1";
    Path path = new Path(OZONE_URI_DELIMITER, testKeyName);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }

    long mtime = 1000;
    fs.setTimes(path, mtime, 2000);

    FileStatus fileStatus = fs.getFileStatus(path);
    // verify that mtime is updated as expected.
    Assert.assertEquals(mtime, fileStatus.getModificationTime());

    long mtimeDontUpdate = -1;
    fs.setTimes(path, mtimeDontUpdate, 2000);

    fileStatus = fs.getFileStatus(path);
    // verify that mtime is NOT updated as expected.
    Assert.assertEquals(mtime, fileStatus.getModificationTime());
  }
}
