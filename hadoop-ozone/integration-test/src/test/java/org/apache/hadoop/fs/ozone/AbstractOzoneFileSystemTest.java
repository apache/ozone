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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_ACLS;
import static org.apache.hadoop.fs.CommonPathCapabilities.FS_CHECKSUMS;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames.OP_CREATE;
import static org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.StorageStatistics.CommonStatisticNames.OP_OPEN;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertHasPathCapabilities;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.fs.ozone.Constants.OZONE_DEFAULT_USER;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.helpers.BucketLayout.FILE_SYSTEM_OPTIMIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.LocatedFileStatus;
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
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractOzoneFileSystemTest extends OzoneFileSystemTestBase {

  private static final float TRASH_INTERVAL = 0.05f; // 3 seconds

  private static final Path ROOT =
      new Path(OZONE_URI_DELIMITER);

  private static final Path TRASH_ROOT =
      new Path(ROOT, TRASH_PREFIX);

  private static final PathFilter EXCLUDE_TRASH =
      p -> !p.toUri().getPath().startsWith(TRASH_ROOT.toString());
  private String fsRoot;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractOzoneFileSystemTest.class);

  private final BucketLayout bucketLayout;
  private final boolean enabledFileSystemPaths;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneManagerProtocol writeClient;
  private OzoneFileSystem fs;
  private OzoneFSStorageStatistics statistics;
  private OzoneBucket ozoneBucket;
  private String volumeName;
  private String bucketName;
  private Trash trash;
  private OMMetrics omMetrics;
  private static final String USER1 = "regularuser1";
  private static final UserGroupInformation UGI_USER1 = UserGroupInformation
      .createUserForTesting(USER1,  new String[] {"usergroup"});
  private OzoneFileSystem userO3fs;

  AbstractOzoneFileSystemTest(boolean setDefaultFs, BucketLayout layout) {
    enabledFileSystemPaths = setDefaultFs;
    bucketLayout = layout;
  }

  @BeforeAll
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);
    conf.setInt(OmConfig.Keys.SERVER_LIST_MAX_SIZE, 2);
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_HBASE_ENHANCEMENTS_ALLOWED, true);
    conf.setBoolean("ozone.client.hbase.enhancements.allowed", true);
    conf.setBoolean(OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_OM_LEASE_SOFT_LIMIT, "0s");
    if (!bucketLayout.equals(FILE_SYSTEM_OPTIMIZED)) {
      conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
          enabledFileSystemPaths);
    }
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        bucketLayout.name());
    cluster = MiniOzoneCluster.newBuilder(conf)
            .setNumDatanodes(5)
            .build();
    cluster.waitForClusterToBeReady();
    omMetrics = cluster.getOzoneManager().getMetrics();

    client = cluster.newClient();
    writeClient = client.getObjectStore()
        .getClientProxy().getOzoneManagerClient();
    // create a volume and a bucket to be used by OzoneFileSystem
    ozoneBucket = TestDataUtil.createVolumeAndBucket(client, bucketLayout);
    volumeName = ozoneBucket.getVolumeName();
    bucketName = ozoneBucket.getName();

    fsRoot = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, bucketName, volumeName);

    // Set the fs.defaultFS and start the filesystem
    conf.set(FS_DEFAULT_NAME_KEY, fsRoot);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    trash = new Trash(conf);
    fs = assertInstanceOf(OzoneFileSystem.class, FileSystem.get(conf));
    statistics = (OzoneFSStorageStatistics) fs.getOzoneFSOpsCountStatistics();
    assertEquals(OzoneConsts.OZONE_URI_SCHEME, fs.getUri().getScheme());
    assertEquals(OzoneConsts.OZONE_URI_SCHEME, statistics.getScheme());

    userO3fs = UGI_USER1.doAs(
        (PrivilegedExceptionAction<OzoneFileSystem>)()
            -> (OzoneFileSystem) FileSystem.get(conf));
  }

  @AfterAll
  void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @AfterEach
  public void cleanup() {
    try {
      deleteRootDir();
    } catch (IOException | InterruptedException ex) {
      LOG.error("Failed to cleanup files.", ex);
      fail("Failed to cleanup files.");
    }
  }

  public MiniOzoneCluster getCluster() {
    return cluster;
  }

  @Override
  public OzoneFileSystem getFs() {
    return fs;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  @Test
  void testUserHomeDirectory() {
    assertEquals(new Path(fsRoot + "user/" + USER1),
        userO3fs.getHomeDirectory());
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
      assertNotNull(outputStream, "Should be able to create file");
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
      assertNotNull(outputStream2, "Should be able to create file");
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
    assertEquals("/d1/d2/d3/d4", fileStatus.getPath().toUri().getPath());
    assertTrue(fileStatus.isDirectory());

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
      assertNotNull(outputStream, "Should be able to create file");
    }

    Path subdir = new Path("/d1/d2/");
    boolean status = fs.mkdirs(subdir);
    assertTrue(status, "Shouldn't send error if dir exists");
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
    TestDataUtil.createKey(ozoneBucket, fullKeyName, new byte[0]);

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
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

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
    InvalidPathException pathException = GenericTestUtils.assertThrows(
        InvalidPathException.class, () -> fs.create(path, false)
    );
    assertThat(pathException.getMessage()).contains("Invalid path Name");
  }

  @Test
  public void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path("/testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    OzoneKeyDetails key = getKey(child, false);
    assertEquals(key.getName(), fs.pathToKey(child));

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (OMException ome) {
      assertEquals(KEY_NOT_FOUND, ome.getResult());
    }

    // List status on the parent should show the child file
    assertEquals(1L, fs.listStatus(parent).length, "List status of parent should include the 1 child file");
    assertTrue(fs.getFileStatus(parent).isDirectory(), "Parent directory does not appear to be a directory");
  }

  @Test
  public void testCreateKeyWithECReplicationConfig() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    createKeyWithECReplicationConfig(root, cluster.getConf());
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
    } catch (OMException ome) {
      assertEquals(KEY_NOT_FOUND, ome.getResult());
    }

    // Delete the child key
    assertTrue(fs.delete(child, false));

    // Deleting the only child should create the parent dir key if it does
    // not exist
    FileStatus fileStatus = fs.getFileStatus(parent);
    assertTrue(fileStatus.isDirectory());
    assertEquals(parent.toString(), fileStatus.getPath().toUri().getPath());

    // Recursive delete with DeleteIterator
    assertTrue(fs.delete(grandparent, true));
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
      assertThat(parents.length).isGreaterThan(0);
      fs.delete(parents[0].getPath(), false);
      fail("Must throw exception as dir is not empty!");
    } catch (PathIsNotEmptyDirectoryException pde) {
      // expected
    }

    // delete a dir with sub-file
    try {
      fs.delete(grandparent, false);
      fail("Must throw exception as dir is not empty!");
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
      assertInstanceOf(FileNotFoundException.class, ex);
      assertThat(ex.getMessage()).contains("No such file or directory");
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
    assertFalse(fs.exists(grandparent));
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      // Make sure all keys under testBatchDelete/parent should be deleted
      assertFalse(fs.exists(childFile));

      // Test to recursively delete child folder, make sure all keys under
      // testBatchDelete/parent/childFolder should be deleted.
      Path childFolderFile = new Path(childFolder, "child" + i);
      assertFalse(fs.exists(childFolderFile));
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

    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    assertEquals(0, fileStatuses.length, "Should be empty");

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    assertEquals(1, fileStatuses.length, "Should have created parent");
    assertEquals(fileStatuses[0].getPath().toUri().getPath(), parent.toString(), "Parent path doesn't match");

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = fs.listStatus(parent);
    assertEquals(2, fileStatuses.length, "FileStatus did not return all children of the directory");

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    fileStatuses = fs.listStatus(parent);
    assertEquals(3, fileStatuses.length, "FileStatus did not return all children of the directory");
  }

  @Test
  public void testObjectOwner() throws Exception {
    // Save the old user, and switch to the old user after test
    UserGroupInformation oldUser = UserGroupInformation.getCurrentUser();
    try {
      // user1 create file /file1
      // user2 create directory /dir1
      // user3 create file /dir1/file2
      UserGroupInformation user1 = UserGroupInformation
          .createUserForTesting("user1", new String[] {"user1"});
      UserGroupInformation user2 = UserGroupInformation
          .createUserForTesting("user2", new String[] {"user2"});
      UserGroupInformation user3 = UserGroupInformation
          .createUserForTesting("user3", new String[] {"user3"});
      Path root = new Path("/");
      Path file1 = new Path(root, "file1");
      Path dir1 = new Path(root, "dir1");
      Path file2 = new Path(dir1, "file2");
      FileStatus[] fileStatuses = fs.listStatus(root);
      assertEquals(0, fileStatuses.length);

      UserGroupInformation.setLoginUser(user1);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      ContractTestUtils.touch(fs, file1);
      UserGroupInformation.setLoginUser(user2);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      fs.mkdirs(dir1);
      UserGroupInformation.setLoginUser(user3);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      ContractTestUtils.touch(fs, file2);

      assertEquals(2, fs.listStatus(root).length);
      assertEquals(1, fs.listStatus(dir1).length);
      assertEquals(user1.getShortUserName(),
          fs.getFileStatus(file1).getOwner());
      assertEquals(user2.getShortUserName(),
          fs.getFileStatus(dir1).getOwner());
      assertEquals(user3.getShortUserName(),
          fs.getFileStatus(file2).getOwner());
    } finally {
      UserGroupInformation.setLoginUser(oldUser);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
    }
  }

  @Test
  public void testObjectProxyUser() throws Exception {
    // Save the old user, and switch to the old user after test
    UserGroupInformation oldUser = UserGroupInformation.getCurrentUser();
    try {
      // user1ProxyUser create file /file1
      // user2ProxyUser create directory /dir1
      // user3ProxyUser create file /dir1/file2
      String proxyUserName = "proxyuser";
      UserGroupInformation proxyuser = UserGroupInformation
          .createUserForTesting(proxyUserName, new String[] {"user1"});
      Path root = new Path("/");
      Path file1 = new Path(root, "file1");
      Path dir1 = new Path(root, "dir1");
      Path file2 = new Path(dir1, "file2");

      UserGroupInformation user1ProxyUser =
          UserGroupInformation.createProxyUser("user1", proxyuser);
      UserGroupInformation user2ProxyUser =
          UserGroupInformation.createProxyUser("user2", proxyuser);
      UserGroupInformation user3ProxyUser =
          UserGroupInformation.createProxyUser("user3", proxyuser);
      FileStatus[] fileStatuses = fs.listStatus(root);
      assertEquals(0, fileStatuses.length);

      UserGroupInformation.setLoginUser(user1ProxyUser);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      ContractTestUtils.touch(fs, file1);
      UserGroupInformation.setLoginUser(user2ProxyUser);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      fs.mkdirs(dir1);
      UserGroupInformation.setLoginUser(user3ProxyUser);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
      ContractTestUtils.touch(fs, file2);

      assertEquals(2, fs.listStatus(root).length);
      assertEquals(1, fs.listStatus(dir1).length);
      assertEquals(user1ProxyUser.getShortUserName(),
          fs.getFileStatus(file1).getOwner());
      assertEquals(user2ProxyUser.getShortUserName(),
          fs.getFileStatus(dir1).getOwner());
      assertEquals(user3ProxyUser.getShortUserName(),
          fs.getFileStatus(file2).getOwner());
    } finally {
      UserGroupInformation.setLoginUser(oldUser);
      fs = (OzoneFileSystem) FileSystem.get(cluster.getConf());
    }
  }

  @Test
  public void testListStatusWithIntermediateDir() throws Exception {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    String keyName = "object-dir/object-name";
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setLocationInfoList(new ArrayList<>())
        .setOwnerName("user" + RandomStringUtils.secure().nextNumeric(5))
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
          fail("listStatus() Failed");
          return false;
        }
      }, 1000, 120000);
    }

    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);

    // the number of immediate children of root is 1
    assertEquals(1, fileStatuses.length, Arrays.toString(fileStatuses));
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
            .setOwnerName(
                UserGroupInformation.getCurrentUser().getShortUserName())
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
          fail("listStatus() Failed");
          return false;
        }
      }, 1000, 120000);
    }
    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    // the number of immediate children of root is 1
    assertEquals(1, fileStatuses.length);
    assertEquals(fileStatuses[0].isErasureCoded(), !bucketLayout.isFileSystemOptimized());
    fileStatuses = fs.listStatus(new Path(
            fileStatuses[0].getPath().toString() + "/object-name1"));
    assertEquals(1, fileStatuses.length);
    assertTrue(fileStatuses[0].isErasureCoded());
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
    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    assertEquals(2, fileStatuses.length, "FileStatus should return only the immediate children");

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
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    deleteRootDir(); // cleanup
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for (int i = 0; i < numDirs; i++) {
      Path p = new Path(ROOT, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
    }

    FileStatus[] fileStatuses = fs.listStatus(ROOT, EXCLUDE_TRASH);
    // Added logs for debugging failures, to check any sub-path mismatches.
    Set<String> actualPaths = new TreeSet<>();
    ArrayList<String> actualPathList = new ArrayList<>();
    if (numDirs != fileStatuses.length) {
      for (FileStatus fileStatus : fileStatuses) {
        boolean duplicate = actualPaths.add(fileStatus.getPath().getName());
        if (!duplicate) {
          LOG.info("Duplicate path:{} in FileStatusList", fileStatus.getPath().getName());
        }
        actualPathList.add(fileStatus.getPath().getName());
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
    assertEquals(numDirs, fileStatuses.length, "Total directories listed do not match the existing directories");

    for (int i = 0; i < numDirs; i++) {
      assertThat(paths).contains(fileStatuses[i].getPath().getName());
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
    TestDataUtil.createKey(ozoneBucket, keyName, new byte[0]);
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
    assertEquals("/dir1/dir2/key1", fileStatuses[0].getPath().toUri().getPath());
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
      assertEquals(0, fileStatuses.length, "Delete root failed!");
    }
  }

  private void deleteRootRecursively(FileStatus[] fileStatuses)
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

    FileStatus[] fileStatuses = fs.listStatus(dir1);
    assertEquals(2, fileStatuses.length, "FileStatus should return only the immediate children");

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
    listStatusIteratorWithDir(ROOT);
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
      RemoteIterator<FileStatus> it = fs.listStatusIterator(ROOT);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        // Verify that dir12 is not included in the result
        // of the listStatusIterator on root.
        assertNotEquals(fileStatus.getPath().toUri().getPath(), dir12.toString());
      }
      assertEquals(2, iCount, "FileStatus should return only the immediate children");
    } finally {
      // Cleanup
      fs.delete(dir2, true);
      fs.delete(dir1, true);
    }
  }

  @Test
  public void testListStatusIteratorOnPageSize() throws Exception {
    listStatusIteratorOnPageSize(cluster.getConf(),
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

      RemoteIterator<FileStatus> it = fs.listStatusIterator(dir1);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        assertTrue(fileStatus.getPath().toUri().getPath().
            equals(dir11.toString()) || fileStatus.getPath().toUri().getPath()
            .equals(dir12.toString()));
      }
      assertEquals(2, iCount, "FileStatus should return only the immediate children");
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
      fail("Should throw FileNotFoundException as file doesn't exist!");
    } catch (FileNotFoundException fnfe) {
      assertThat(fnfe.getMessage()).contains("KEY_NOT_FOUND");
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
      assertThat(fileLength)
          .withFailMessage("Block allocation should happen")
          .isGreaterThan(blkSize);

      long newNumBlockAllocations =
              cluster.getOzoneManager().getMetrics().getNumBlockAllocates();

      assertThat(newNumBlockAllocations)
          .withFailMessage("Block allocation should happen")
          .isGreaterThan(numBlockAllocationsOrg);

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
    assertNotNull(statuses, "liststatus returns a null array");
    assertEquals(0, statuses.length, "Statuses array is not empty");
    FileStatus fileStatus = fs.getFileStatus(interimPath);
    assertEquals(interimPath.getName(), fileStatus.getPath().getName(), "FileStatus does not point to interimPath");
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
    assertFalse(fs.rename(source, destin), "Expected to fail rename as src doesn't exist");
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
      fail("Should throw exception : Cannot rename a directory to" +
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
    assertTrue(fs.rename(aSourcePath, bDestinPath), "Rename failed");

    final Path baPath = new Path(fs.getUri().toString() + "/b/a");
    final Path bacPath = new Path(fs.getUri().toString() + "/b/a/c");
    assertTrue(fs.exists(baPath), "Rename failed");
    assertTrue(fs.exists(bacPath), "Rename failed");
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

    assertFalse(fs.rename(aSourcePath, bDestinPath), "New destin sub-path /b/a already exists");

    // Case-5.b) Rename file from /a/b/c/file1 to /a.
    // Should be failed since /a/file1 exists.
    final Path abcPath = new Path(fs.getUri().toString() + "/a/b/c");
    fs.mkdirs(abcPath);
    Path abcFile1 = new Path(abcPath, "/file1");
    ContractTestUtils.touch(fs, abcFile1);

    final Path aFile1 = new Path(fs.getUri().toString() + "/a/file1");
    ContractTestUtils.touch(fs, aFile1);

    final Path aDestinPath = new Path(fs.getUri().toString() + "/a");

    assertFalse(fs.rename(abcFile1, aDestinPath), "New destin sub-path /b/a already exists");
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
    assertFalse(fs.rename(abcRootPath, file1Destin), "key already exists /root_dir/file1");
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
    assertTrue(fs.rename(file1Source, file1Destin), "Renamed failed");
    assertTrue(fs.exists(file1Destin), "Renamed failed: /root/file1");

    /*
     * Reading several times, this is to verify that OmKeyInfo#keyName cached
     * entry is not modified. While reading back, OmKeyInfo#keyName will be
     * prepared and assigned to fullkeyPath name.
     */
    for (int i = 0; i < 10; i++) {
      FileStatus[] fStatus = fs.listStatus(rootPath);
      assertEquals(1, fStatus.length, "Renamed failed");
      assertEquals(file1Destin, fStatus[0].getPath(), "Wrong path name!");
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
    assertTrue(fs.rename(file1Destin, abcRootPath), "Renamed failed");
    assertTrue(fs.exists(new Path(abcRootPath,
            "file1")), "Renamed filed: /a/b/c/file1");
  }

  @Test
  public void testRenameContainDelimiterFile() throws Exception {
    String fakeGrandpaKey = "dir1";
    String fakeParentKey = fakeGrandpaKey + "/dir2";
    String sourceKeyName = fakeParentKey + "/key1";
    String targetKeyName = fakeParentKey +  "/key2";
    TestDataUtil.createKey(ozoneBucket, sourceKeyName, new byte[0]);

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
      fail("Should fail as parent of dst does not exist!");
    } catch (FileNotFoundException fnfe) {
      // expected
    }

    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(fs.getUri().toString() + root + "/file1");
    ContractTestUtils.touch(fs, filePath);

    Path newDestinPath = new Path(filePath, "c");
    try {
      fs.rename(dir2SourcePath, newDestinPath);
      fail("Should fail as parent of dst is a file!");
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
    assertTrue(fs.rename(dir2SourcePath, destRootPath), "Rename failed");
    final Path expectedPathAfterRename =
            new Path(fs.getUri().toString() + root + "/dir2");
    assertTrue(fs.exists(expectedPathAfterRename), "Rename failed");

    // rename source file to its parent directory(destination).
    assertTrue(fs.rename(file1Source, destRootPath), "Rename failed");
    final Path expectedFilePathAfterRename =
            new Path(fs.getUri().toString() + root + "/file2");
    assertTrue(fs.exists(expectedFilePathAfterRename), "Rename failed");
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
    assertTrue(fs.exists(dest), "Directory rename failed");
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue(fs.exists(new Path(dest, "sub_dir1")), "Keys under the renamed directory not renamed");

    // Test if one path belongs to other FileSystem.
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> fs.rename(new Path(fs.getUri().toString() + "fake" + dir), dest));
    assertThat(exception.getMessage()).contains("Wrong FS");
  }

  @Override
  protected OzoneKeyDetails getKey(Path keyPath, boolean isDirectory)
      throws IOException {
    String key = fs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + "/";
    }
    return client.getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(key);
  }

  @Test
  public void testGetDirectoryModificationTime()
      throws IOException, InterruptedException {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    Path mdir1 = new Path("/mdir1");
    Path mdir11 = new Path(mdir1, "mdir11");
    Path mdir111 = new Path(mdir11, "mdir111");
    fs.mkdirs(mdir111);

    // Case 1: Dir key exist on server
    FileStatus[] fileStatuses = fs.listStatus(mdir11);
    // Above listStatus result should only have one entry: mdir111
    assertEquals(1, fileStatuses.length);
    assertEquals(mdir111.toString(), fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());
    // The dir key is actually created on server,
    // so modification time should always be the same value.
    long modificationTime = fileStatuses[0].getModificationTime();
    // Check modification time in a small loop, it should always be the same
    for (int i = 0; i < 5; i++) {
      Thread.sleep(10);
      fileStatuses = fs.listStatus(mdir11);
      assertEquals(modificationTime, fileStatuses[0].getModificationTime());
    }

    // Case 2: Dir key doesn't exist on server
    fileStatuses = fs.listStatus(mdir1);
    // Above listStatus result should only have one entry: mdir11
    assertEquals(1, fileStatuses.length);
    assertEquals(mdir11.toString(), fileStatuses[0].getPath().toUri().getPath());
    assertTrue(fileStatuses[0].isDirectory());
    // Since the dir key doesn't exist on server, the modification time is
    // set to current time upon every listStatus request.
    modificationTime = fileStatuses[0].getModificationTime();
    // Check modification time in a small loop, it should be slightly larger
    // each time
    for (int i = 0; i < 5; i++) {
      Thread.sleep(10);
      fileStatuses = fs.listStatus(mdir1);
      assertThat(modificationTime).isLessThanOrEqualTo(fileStatuses[0].getModificationTime());
    }
  }

  @Test
  public void testGetTrashRoot() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    // Input path doesn't matter, o3fs.getTrashRoot() only cares about username
    Path inPath1 = new Path("o3fs://bucket2.volume1/path/to/key");
    // Test with current user
    Path outPath1 = fs.getTrashRoot(inPath1);
    Path expectedOutPath1 = fs.makeQualified(new Path(TRASH_ROOT, username));
    assertEquals(expectedOutPath1, outPath1);
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
    conf.set(FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    try (FileSystem fileSystem = FileSystem.get(conf)) {
      OzoneFileSystem o3FS = (OzoneFileSystem) fileSystem;

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
      bucket.setReplicationConfig(RatisReplicationConfig.getInstance(
          HddsProtos.ReplicationFactor.THREE));

      createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key3"),
          ReplicationType.EC);

      testClock.fastForward(300 * 1000 + 1);

      createKeyAndAssertKeyType(bucket, o3FS, new Path(rootPath, "key4"),
          ReplicationType.RATIS);
    }
  }

  private void createKeyAndAssertKeyType(OzoneBucket bucket,
      OzoneFileSystem o3FS, Path keyPath, ReplicationType expectedType)
      throws IOException {
    o3FS.createFile(keyPath).build().close();
    assertEquals(expectedType.name(), bucket.getKey(o3FS.pathToKey(keyPath)).getReplicationConfig()
        .getReplicationType().name());
  }

  @Test
  public void testGetTrashRoots() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userTrash = new Path(TRASH_ROOT, username);

    Collection<FileStatus> res = fs.getTrashRoots(false);
    assertEquals(0, res.size());

    fs.mkdirs(userTrash);
    res = fs.getTrashRoots(false);
    assertEquals(1, res.size());
    res.forEach(e -> assertEquals(userTrash.toString(), e.getPath().toUri().getPath()));
    // Only have one user trash for now
    res = fs.getTrashRoots(true);
    assertEquals(1, res.size());

    // Create a few more random user trash dir
    for (int i = 1; i <= 5; i++) {
      Path moreUserTrash = new Path(TRASH_ROOT, "trashuser" + i);
      fs.mkdirs(moreUserTrash);
    }

    // And create a file, which should be ignored
    fs.create(new Path(TRASH_ROOT, "trashuser99"));

    // allUsers = false should still return current user trash
    res = fs.getTrashRoots(false);
    assertEquals(1, res.size());
    res.forEach(e -> assertEquals(userTrash.toString(), e.getPath().toUri().getPath()));
    // allUsers = true should return all user trash
    res = fs.getTrashRoots(true);
    assertEquals(6, res.size());
  }

  @Test
  public void testDeleteRootWithTrash() throws IOException {
    // Try to delete root
    Path root = new Path(OZONE_URI_DELIMITER);
    assertThrows(IOException.class, () -> trash.moveToTrash(root));
    // Also try with TrashPolicyDefault
    OzoneConfiguration conf2 = new OzoneConfiguration(cluster.getConf());
    conf2.setClass("fs.trash.classname", TrashPolicyDefault.class,
        TrashPolicy.class);
    try (FileSystem fs = FileSystem.get(conf2)) {
      Trash trashPolicyDefault = new Trash(fs, conf2);
      assertThrows(IOException.class,
          () -> trashPolicyDefault.moveToTrash(root));
    }
  }

  /**
   * 1.Move a Key to Trash
   * 2.Verify that the key gets deleted by the trash emptier.
   */
  @Test
  public void testTrash() throws Exception {
    String testKeyName = "testKey2";
    Path path = new Path(OZONE_URI_DELIMITER, testKeyName);
    ContractTestUtils.touch(fs, path);
    assertTrue(trash.getConf().getClass(
        "fs.trash.classname", TrashPolicy.class).
        isAssignableFrom(OzoneTrashPolicy.class));
    assertEquals(TRASH_INTERVAL, trash.getConf().
        getFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, 0), 0);

    // Construct paths
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path userTrash = new Path(TRASH_ROOT, username);
    Path userTrashCurrent = new Path(userTrash, "Current");
    Path trashPath = new Path(userTrashCurrent, testKeyName);
    assertFalse(fs.exists(userTrash));

    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(path);

    assertTrue(fs.exists(userTrash));
    assertTrue(fs.exists(trashPath) || fs.listStatus(
        fs.listStatus(userTrash)[0].getPath()).length > 0);

    // Wait until the TrashEmptier purges the key
    GenericTestUtils.waitFor(() -> {
      try {
        return !fs.exists(trashPath);
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed");
        fail("Delete from Trash Failed");
        return false;
      }
    }, 100, 120000);

    // wait for deletion of checkpoint dir
    GenericTestUtils.waitFor(() -> {
      try {
        return fs.listStatus(userTrash).length == 0;
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed", e);
        fail("Delete from Trash Failed");
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

    // unknown keyName
    OMException ome = assertThrows(OMException.class,
        () -> new OzonePrefixPathImpl(getVolumeName(), getBucketName(), "invalidKey",
            cluster.getOzoneManager().getKeyManager()));
    assertEquals(KEY_NOT_FOUND, ome.getResult());

    OzonePrefixPathImpl ozonePrefixPath =
        new OzonePrefixPathImpl(getVolumeName(), getBucketName(), keyName,
            cluster.getOzoneManager().getKeyManager());

    OzoneFileStatus status = ozonePrefixPath.getOzoneFileStatus();
    assertNotNull(status);
    assertEquals(keyName, status.getTrimmedName());
    assertTrue(status.isDirectory());

    Iterator<? extends OzoneFileStatus> pathItr =
        ozonePrefixPath.getChildren(keyName);
    assertTrue(pathItr.hasNext(), "Failed to list keyPath:" + keyName);

    Set<String> actualPaths = new TreeSet<>();
    while (pathItr.hasNext()) {
      String pathname = pathItr.next().getTrimmedName();
      actualPaths.add(pathname);

      // no subpaths, expected an empty list
      Iterator<? extends OzoneFileStatus> subPathItr =
          ozonePrefixPath.getChildren(pathname);
      assertNotNull(subPathItr);
      assertFalse(subPathItr.hasNext(), "Failed to list keyPath: " + pathname);
    }

    assertEquals(paths.size(), actualPaths.size(), "ListStatus failed");

    for (String pathname : actualPaths) {
      paths.remove(pathname);
    }
    assertTrue(paths.isEmpty(), "ListStatus failed:" + paths);
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
    assertEquals(mtime, fileStatus.getModificationTime());

    long mtimeDontUpdate = -1;
    fs.setTimes(path, mtimeDontUpdate, 2000);

    fileStatus = fs.getFileStatus(path);
    // verify that mtime is NOT updated as expected.
    assertEquals(mtime, fileStatus.getModificationTime());
  }

  @Test
  public void testLoopInLinkBuckets() throws Exception {
    String linksVolume = UUID.randomUUID().toString();

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

    String rootPath = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, linkBucket1Name, linksVolume);

    try (FileSystem fileSystem = FileSystem.get(URI.create(rootPath),
        cluster.getConf())) {
      fail("Should throw Exception due to loop in Link Buckets" +
          " while initialising fs with URI " + fileSystem.getUri());
    } catch (OMException oe) {
      // Expected exception
      assertEquals(OMException.ResultCodes.DETECTED_LOOP_IN_BUCKET_LINKS,
          oe.getResult());
    } finally {
      volume.deleteBucket(linkBucket1Name);
      volume.deleteBucket(linkBucket2Name);
      volume.deleteBucket(linkBucket3Name);
    }

    // case-2: Dangling link bucket
    String danglingLinkBucketName = UUID.randomUUID().toString();
    String sourceBucketName = UUID.randomUUID().toString();

    // danglingLinkBucket is a dangling link over a source bucket that doesn't
    // exist.
    createLinkBucket(volume, sourceBucketName, danglingLinkBucketName);

    String rootPath2 = String.format("%s://%s.%s/",
        OzoneConsts.OZONE_URI_SCHEME, danglingLinkBucketName, linksVolume);

    FileSystem fileSystem = null;
    try {
      fileSystem = FileSystem.get(URI.create(rootPath2), cluster.getConf());
    } catch (OMException oe) {
      // Expected exception
      fail("Should not throw Exception and show orphan buckets");
    } finally {
      volume.deleteBucket(danglingLinkBucketName);
      if (fileSystem != null) {
        fileSystem.close();
      }
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

  @Test
  public void testProcessingDetails() throws IOException, InterruptedException {
    final Logger log = LoggerFactory.getLogger(
        "org.apache.hadoop.ipc_.ProcessingDetails");
    GenericTestUtils.setLogLevel(log, Level.DEBUG);
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(log);
    int keySize = 1024;
    TestDataUtil.createKey(ozoneBucket, "key1", new byte[keySize]);
    logCapturer.stopCapturing();
    String logContent = logCapturer.getOutput();

    int nonZeroLines = 0;
    for (String s: logContent.split("\n")) {
      // The following conditions means write operations from Clients.
      if (!s.contains("lockexclusiveTime=0") &&
          s.contains("OzoneManagerProtocol")) {
        nonZeroLines++;
      }
    }

    GenericTestUtils.setLogLevel(log, Level.INFO);
    assertNotEquals(nonZeroLines, 0);
  }

  @Test
  public void testOzFsReadWrite() throws IOException {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    long currentTime = Time.now();
    int stringLen = 20;
    OMMetadataManager metadataManager = cluster.getOzoneManager()
        .getMetadataManager();
    String lev1dir = "l1dir";
    Path lev1path = createPath("/" + lev1dir);
    String lev1key = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(lev1path));
    String lev2dir = "l2dir";
    Path lev2path = createPath("/" + lev1dir + "/" + lev2dir);
    String lev2key = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(lev2path));

    String data = RandomStringUtils.secure().nextAlphanumeric(stringLen);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);

    Path path = createPath("/" + lev1dir + "/" + lev2dir + "/" + filePath);
    String fileKey = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(path));

    // verify prefix directories and the file, do not already exist
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(lev1key));
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(lev2key));
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(fileKey));

    Map<String, Long> statsBefore = statistics.snapshot();
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }

    assertChange(statsBefore, statistics, OP_CREATE, 1);
    assertChange(statsBefore, statistics, "objects_created", 1);

    FileStatus status = fs.getFileStatus(path);

    assertChange(statsBefore, statistics, OP_GET_FILE_STATUS, 1);
    assertChange(statsBefore, statistics, "objects_query", 1);

    // The timestamp of the newly created file should always be greater than
    // the time when the test was started
    assertThat(status.getModificationTime()).isGreaterThan(currentTime);

    assertFalse(status.isDirectory());
    assertEquals(FsPermission.getFileDefault(), status.getPermission());
    verifyOwnerGroup(status);

    // verify prefix directories got created when creating the file.
    assertEquals("l1dir/", metadataManager.getKeyTable(getBucketLayout()).get(lev1key).getKeyName());
    assertEquals("l1dir/l2dir/", metadataManager.getKeyTable(getBucketLayout()).get(lev2key).getKeyName());
    FileStatus lev1status = getDirectoryStat(lev1path);
    assertNotNull(lev1status);
    FileStatus lev2status = getDirectoryStat(lev2path);
    assertNotNull(lev2status);

    try (FSDataInputStream inputStream = fs.open(path)) {
      byte[] buffer = new byte[stringLen];
      // This read will not change the offset inside the file
      int readBytes = inputStream.read(0, buffer, 0, buffer.length);
      String out = new String(buffer, 0, buffer.length, UTF_8);
      assertEquals(data, out);
      assertEquals(readBytes, buffer.length);
      assertEquals(0, inputStream.getPos());

      // The following read will change the internal offset
      readBytes = inputStream.read(buffer, 0, buffer.length);
      assertEquals(data, out);
      assertEquals(readBytes, buffer.length);
      assertEquals(buffer.length, inputStream.getPos());
    }

    assertChange(statsBefore, statistics, OP_OPEN, 1);
    assertChange(statsBefore, statistics, "objects_read", 1);
  }

  private static void assertChange(Map<String, Long> before, OzoneFSStorageStatistics after, String key, long delta) {
    assertEquals(before.get(key) + delta, after.getLong(key));
  }

  @Test
  public void testReplication() throws IOException {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    int stringLen = 20;
    String data = RandomStringUtils.secure().nextAlphanumeric(stringLen);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);

    Path pathIllegal = createPath("/" + filePath + "illegal");
    try (FSDataOutputStream streamIllegal = fs.create(pathIllegal, (short)2)) {
      streamIllegal.writeBytes(data);
    }
    assertEquals(3, fs.getFileStatus(pathIllegal).getReplication());

    Path pathLegal = createPath("/" + filePath + "legal");
    try (FSDataOutputStream streamLegal = fs.create(pathLegal, (short)1)) {
      streamLegal.writeBytes(data);
    }
    assertEquals(1, fs.getFileStatus(pathLegal).getReplication());
  }

  private void verifyOwnerGroup(FileStatus fileStatus) {
    String owner = getCurrentUser();
    assertEquals(owner, fileStatus.getOwner());
    assertEquals(owner, fileStatus.getGroup());
  }

  @Test
  public void testDirectory() throws IOException {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    String leafName = RandomStringUtils.secure().nextAlphanumeric(5);
    OMMetadataManager metadataManager = cluster.getOzoneManager()
        .getMetadataManager();

    String lev1dir = "abc";
    Path lev1path = createPath("/" + lev1dir);
    String lev1key = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(lev1path));
    String lev2dir = "def";
    Path lev2path = createPath("/" + lev1dir + "/" + lev2dir);
    String lev2key = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(lev2path));

    Path leaf = createPath("/" + lev1dir + "/" + lev2dir + "/" + leafName);
    String leafKey = metadataManager.getOzoneDirKey(volumeName, bucketName,
        fs.pathToKey(leaf));

    // verify prefix directories and the leaf, do not already exist
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(lev1key));
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(lev2key));
    assertNull(metadataManager.getKeyTable(getBucketLayout()).get(leafKey));

    assertTrue(fs.mkdirs(leaf));

    // verify the leaf directory got created.
    FileStatus leafstatus = getDirectoryStat(leaf);
    assertNotNull(leafstatus);

    // verify prefix directories got created when creating the leaf directory.
    assertEquals("abc/", metadataManager
        .getKeyTable(getBucketLayout())
        .get(lev1key)
        .getKeyName());
    assertEquals("abc/def/", metadataManager
        .getKeyTable(getBucketLayout())
        .get(lev2key)
        .getKeyName());
    FileStatus lev1status = getDirectoryStat(lev1path);
    assertNotNull(lev1status);
    FileStatus lev2status = getDirectoryStat(lev2path);
    assertNotNull(lev2status);

    // check the root directory
    FileStatus rootStatus = getDirectoryStat(createPath("/"));
    assertNotNull(rootStatus);

    // root directory listing should contain the lev1 prefix directory
    FileStatus[] statusList = fs.listStatus(createPath("/"));
    assertEquals(1, statusList.length);
    assertEquals(lev1status, statusList[0]);
  }

  @Test
  void testListStatus2() throws IOException {
    List<Path> paths = new ArrayList<>();
    String dirPath = RandomStringUtils.secure().nextAlphanumeric(5);
    Path path = createPath("/" + dirPath);
    paths.add(path);

    final Map<String, Long> initialStats = statistics.snapshot();
    assertTrue(fs.mkdirs(path));
    assertChange(initialStats, statistics, OP_MKDIRS, 1);

    final long initialListStatusCount = omMetrics.getNumListStatus();
    FileStatus[] statusList = fs.listStatus(createPath("/"));
    assertEquals(1, statusList.length);
    assertChange(initialStats, statistics, Statistic.OBJECTS_LIST.getSymbol(), 2);
    assertEquals(initialListStatusCount + 2, omMetrics.getNumListStatus());
    assertEquals(fs.getFileStatus(path), statusList[0]);

    dirPath = RandomStringUtils.secure().nextAlphanumeric(5);
    path = createPath("/" + dirPath);
    paths.add(path);
    assertTrue(fs.mkdirs(path));
    assertChange(initialStats, statistics, OP_MKDIRS, 2);

    statusList = fs.listStatus(createPath("/"));
    assertEquals(2, statusList.length);
    assertChange(initialStats, statistics, Statistic.OBJECTS_LIST.getSymbol(), 4);
    assertEquals(initialListStatusCount + 4, omMetrics.getNumListStatus());
    for (Path p : paths) {
      assertThat(Arrays.asList(statusList)).contains(fs.getFileStatus(p));
    }
  }

  @Test
  public void testOzoneManagerListLocatedStatusAndListStatus() throws IOException {
    String data = RandomStringUtils.secure().nextAlphanumeric(20);
    String directory = RandomStringUtils.secure().nextAlphanumeric(5);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);
    Path path = createPath("/" + directory + "/" + filePath);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }
    RemoteIterator<LocatedFileStatus> listLocatedStatus = fs.listLocatedStatus(path);
    int count = 0;
    while (listLocatedStatus.hasNext()) {
      LocatedFileStatus locatedFileStatus = listLocatedStatus.next();
      assertTrue(locatedFileStatus.getBlockLocations().length >= 1);

      for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()) {
        assertTrue(blockLocation.getNames().length >= 1);
        assertTrue(blockLocation.getHosts().length >= 1);
      }
      count++;
    }
    assertEquals(1, count);
    count = 0;
    RemoteIterator<FileStatus> listStatus = fs.listStatusIterator(path);
    while (listStatus.hasNext()) {
      FileStatus fileStatus = listStatus.next();
      assertFalse(fileStatus instanceof LocatedFileStatus);
      count++;
    }
    assertEquals(1, count);
    FileStatus[] fileStatuses = fs.listStatus(path.getParent());
    assertEquals(1, fileStatuses.length);
    assertFalse(fileStatuses[0] instanceof LocatedFileStatus);
  }

  @Test
  public void testOzoneManagerListLocatedStatusForZeroByteFile() throws IOException {
    String directory = RandomStringUtils.secure().nextAlphanumeric(5);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);
    Path path = createPath("/" + directory + "/" + filePath);

    listLocatedStatusForZeroByteFile(fs, path);
  }

  @Test
  void testOzoneManagerFileSystemInterface() throws IOException {
    String dirPath = RandomStringUtils.secure().nextAlphanumeric(5);

    Path path = createPath("/" + dirPath);
    assertTrue(fs.mkdirs(path));

    long numFileStatus =
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus();
    FileStatus status = fs.getFileStatus(path);

    assertEquals(numFileStatus + 1,
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus());
    assertTrue(status.isDirectory());
    assertEquals(FsPermission.getDirDefault(), status.getPermission());
    verifyOwnerGroup(status);

    long currentTime = System.currentTimeMillis();
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(fs.pathToKey(path))
        .build();
    OzoneFileStatus omStatus =
        cluster.getOzoneManager().getFileStatus(keyArgs);
    //Another get file status here, incremented the counter.
    assertEquals(numFileStatus + 2,
        cluster.getOzoneManager().getMetrics().getNumGetFileStatus());
    assertTrue(omStatus.isDirectory());

    // For directories, the time returned is the current time when the dir key
    // doesn't actually exist on server; if it exists, it will be a fixed value.
    // In this case, the dir key exists.
    assertEquals(0, omStatus.getKeyInfo().getDataSize());
    assertThat(omStatus.getKeyInfo().getModificationTime()).isLessThanOrEqualTo(currentTime);
    assertEquals(new Path(omStatus.getPath()).getName(),
        fs.pathToKey(path));
  }

  @Test
  public void testOzoneManagerLocatedFileStatus() throws IOException {
    String data = RandomStringUtils.secure().nextAlphanumeric(20);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);
    Path path = createPath("/" + filePath);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }
    FileStatus status = fs.getFileStatus(path);
    LocatedFileStatus locatedFileStatus = assertInstanceOf(LocatedFileStatus.class, status);
    assertThat(locatedFileStatus.getBlockLocations().length).isGreaterThanOrEqualTo(1);

    for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()) {
      assertThat(blockLocation.getNames().length).isGreaterThanOrEqualTo(1);
      assertThat(blockLocation.getHosts().length).isGreaterThanOrEqualTo(1);
    }
  }

  @Test
  void testBlockOffsetsWithMultiBlockFile() throws Exception {
    // naive assumption: MiniOzoneCluster will not have larger than ~1GB
    // block size when running this test.
    int blockSize = (int) fs.getConf().getStorageSize(
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT,
        StorageUnit.BYTES
    );
    String data = RandomStringUtils.secure().nextAlphanumeric(2 * blockSize + 837);
    String filePath = RandomStringUtils.secure().nextAlphanumeric(5);
    Path path = createPath("/" + filePath);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.writeBytes(data);
    }
    FileStatus status = fs.getFileStatus(path);
    LocatedFileStatus locatedFileStatus = assertInstanceOf(LocatedFileStatus.class, status);
    BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();

    assertEquals(0, blockLocations[0].getOffset());
    assertEquals(blockSize, blockLocations[1].getOffset());
    assertEquals(2 * blockSize, blockLocations[2].getOffset());
    assertEquals(blockSize, blockLocations[0].getLength());
    assertEquals(blockSize, blockLocations[1].getLength());
    assertEquals(837, blockLocations[2].getLength());
  }

  @Test
  void testPathToKey() {
    assumeFalse(FILE_SYSTEM_OPTIMIZED.equals(getBucketLayout()));

    assertEquals("a/b/1", fs.pathToKey(new Path("/a/b/1")));

    assertEquals("user/" + getCurrentUser() + "/key1/key2",
        fs.pathToKey(new Path("key1/key2")));

    assertEquals("key1/key2",
        fs.pathToKey(new Path("o3fs://test1/key1/key2")));
  }


  /**
   * Verify that FS throws exception when trying to access bucket with
   * incompatible layout.
   */
  @Test
  void testFileSystemWithObjectStoreLayout() throws IOException {
    String obsVolume = UUID.randomUUID().toString();

    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();

      // Create volume and bucket
      store.createVolume(obsVolume);
      OzoneVolume volume = store.getVolume(obsVolume);
      String obsBucket = UUID.randomUUID().toString();
      // create bucket with OBJECT_STORE bucket layout (incompatible with fs)
      volume.createBucket(obsBucket, BucketArgs.newBuilder()
          .setBucketLayout(BucketLayout.OBJECT_STORE)
          .build());

      String obsRootPath = String.format("%s://%s.%s/",
          OzoneConsts.OZONE_URI_SCHEME, obsBucket, obsVolume);

      OzoneConfiguration config = new OzoneConfiguration(fs.getConf());
      config.set(FS_DEFAULT_NAME_KEY, obsRootPath);

      IllegalArgumentException e = GenericTestUtils.assertThrows(IllegalArgumentException.class,
              () -> FileSystem.get(config));
      assertThat(e.getMessage()).contains("OBJECT_STORE, which does not support file system semantics");
    }
  }

  @Test
  public void testGetFileChecksumWithInvalidCombineMode() throws IOException {
    final String root = "/root";
    Path rootPath = new Path(fs.getUri().toString() + root);
    fs.mkdirs(rootPath);
    Path file = new Path(fs.getUri().toString() + root
        + "/dummy");
    ContractTestUtils.touch(fs, file);
    OzoneClientConfig clientConfig = cluster.getConf().getObject(OzoneClientConfig.class);
    clientConfig.setChecksumCombineMode("NONE");
    OzoneConfiguration conf = cluster.getConf();
    conf.setFromObject(clientConfig);
    conf.setBoolean("fs.o3fs.impl.disable.cache", true);
    try (FileSystem fileSystem = FileSystem.get(conf)) {
      assertNull(fileSystem.getFileChecksum(file));
    }
  }

  private String getCurrentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      return OZONE_DEFAULT_USER;
    }
  }

  private Path createPath(String relativePath) {
    if (enabledFileSystemPaths) {
      return new Path(fsRoot + (relativePath.startsWith("/") ? "" : "/") + relativePath);
    } else {
      return new Path(relativePath);
    }
  }

  /**
   * verify that a directory exists and is initialized correctly.
   * @param path of the directory
   * @return null indicates FILE_NOT_FOUND, else the FileStatus
   */
  private FileStatus getDirectoryStat(Path path) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    assertTrue(status.isDirectory());
    assertEquals(FsPermission.getDirDefault(), status.getPermission());
    verifyOwnerGroup(status);
    assertEquals(0, status.getLen());
    return status;
  }

  @Test
  void testSnapshotRead() throws Exception {
    // Init data
    Path snapPath1 = fs.createSnapshot(new Path("/"), "snap1");

    Path file1 = new Path("/key1");
    Path file2 = new Path("/key2");
    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);
    Path snapPath2 = fs.createSnapshot(new Path("/"), "snap2");

    Path file3 = new Path("/key3");
    ContractTestUtils.touch(fs, file3);
    Path snapPath3 = fs.createSnapshot(new Path("/"), "snap3");

    FileStatus[] f1 = fs.listStatus(snapPath1);
    FileStatus[] f2 = fs.listStatus(snapPath2);
    FileStatus[] f3 = fs.listStatus(snapPath3);
    assertEquals(0, f1.length);
    assertEquals(2, f2.length);
    assertEquals(3, f3.length);
  }
}
