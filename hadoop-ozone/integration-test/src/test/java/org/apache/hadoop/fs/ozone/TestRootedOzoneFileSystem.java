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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.TrashPolicy;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.TrashPolicyOzone;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.FileSystem.LOG;
import static org.apache.hadoop.fs.FileSystem.TRASH_PREFIX;
import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Ozone file system tests that are not covered by contract tests.
 * TODO: Refactor this and TestOzoneFileSystem to reduce duplication.
 */
@RunWith(Parameterized.class)
public class TestRootedOzoneFileSystem {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestRootedOzoneFileSystem(boolean setDefaultFs,
      boolean enableOMRatis) {
    enabledFileSystemPaths = setDefaultFs;
    omRatisEnabled = enableOMRatis;
  }

  @Rule
  public Timeout globalTimeout = new Timeout(300_000);

  private static boolean enabledFileSystemPaths;
  private static boolean omRatisEnabled;

  private static OzoneConfiguration conf;
  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static RootedOzoneFileSystem ofs;
  private static ObjectStore objectStore;
  private static BasicRootedOzoneClientAdapterImpl adapter;
  private static Trash trash;

  private static String volumeName;
  private static Path volumePath;
  private static String bucketName;
  // Store path commonly used by tests that test functionality within a bucket
  private static Path bucketPath;
  private static String rootPath;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 1);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, omRatisEnabled);
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        enabledFileSystemPaths);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = cluster.getClient().getObjectStore();
    
    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    volumeName = bucket.getVolumeName();
    volumePath = new Path(OZONE_URI_DELIMITER, volumeName);
    bucketName = bucket.getName();
    bucketPath = new Path(volumePath, bucketName);

    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);
    // fs.ofs.impl would be loaded from META-INF, no need to manually set it
    fs = FileSystem.get(conf);
    conf.setClass("fs.trash.classname", TrashPolicyOzone.class,
        TrashPolicy.class);
    trash = new Trash(conf);
    ofs = (RootedOzoneFileSystem) fs;
    adapter = (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testOzoneFsServiceLoader() throws IOException {
    OzoneConfiguration confTestLoader = new OzoneConfiguration();
    // fs.ofs.impl should be loaded from META-INF, no need to explicitly set it
    Assert.assertEquals(FileSystem.getFileSystemClass(
        OzoneConsts.OZONE_OFS_URI_SCHEME, confTestLoader),
        RootedOzoneFileSystem.class);
  }

  @Test
  public void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path(bucketPath,
        "testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);

    OzoneKeyDetails key = getKey(child, false);
    OFSPath childOFSPath = new OFSPath(child);
    Assert.assertEquals(key.getName(), childOFSPath.getKeyName());

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // List status on the parent should show the child file
    Assert.assertEquals(
        "List status of parent should include the 1 child file",
        1L, fs.listStatus(parent).length);
    Assert.assertTrue(
        "Parent directory does not appear to be a directory",
        fs.getFileStatus(parent).isDirectory());

    // Cleanup
    fs.delete(grandparent, true);
  }

  @Test
  public void testDeleteCreatesFakeParentDir() throws Exception {
    Path grandparent = new Path(bucketPath,
        "testDeleteCreatesFakeParentDir");
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
    OFSPath parentOFSPath = new OFSPath(parent);
    String parentKey = parentOFSPath.getKeyName() + "/";
    OzoneKeyDetails parentKeyInfo = getKey(parent, true);
    Assert.assertEquals(parentKey, parentKeyInfo.getName());

    // Recursive delete with DeleteIterator
    Assert.assertTrue(fs.delete(grandparent, true));
  }

  @Test
  public void testListStatus() throws Exception {
    Path parent = new Path(bucketPath, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    FileStatus[] fileStatuses = ofs.listStatus(bucketPath);
    Assert.assertEquals("Should be empty", 0, fileStatuses.length);

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = ofs.listStatus(bucketPath);
    Assert.assertEquals("Should have created parent",
        1, fileStatuses.length);
    Assert.assertEquals("Parent path doesn't match",
        fileStatuses[0].getPath().toUri().getPath(), parent.toString());

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    fileStatuses = ofs.listStatus(parent);
    Assert.assertEquals(
        "FileStatus did not return all children of the directory",
        2, fileStatuses.length);

    // ListStatus should return only the immediate children of a directory.
    Path file3 = new Path(parent, "dir1/key3");
    Path file4 = new Path(parent, "dir1/key4");
    ContractTestUtils.touch(fs, file3);
    ContractTestUtils.touch(fs, file4);
    fileStatuses = ofs.listStatus(parent);
    Assert.assertEquals(
        "FileStatus did not return all children of the directory",
        3, fileStatuses.length);

    // Cleanup
    fs.delete(parent, true);
  }

  /**
   * OFS: Helper function for tests. Return a volume name that doesn't exist.
   */
  private String getRandomNonExistVolumeName() throws IOException {
    final int numDigit = 5;
    long retriesLeft = Math.round(Math.pow(10, 5));
    String name = null;
    while (name == null && retriesLeft-- > 0) {
      name = "volume-" + RandomStringUtils.randomNumeric(numDigit);
      // Check volume existence.
      Iterator<? extends OzoneVolume> iter =
          objectStore.listVolumesByUser(null, name, null);
      if (iter.hasNext()) {
        // If there is a match, try again.
        // Note that volume name prefix match doesn't equal volume existence
        //  but the check is sufficient for this test.
        name = null;
      }
    }
    if (retriesLeft <= 0) {
      Assert.fail(
          "Failed to generate random volume name that doesn't exist already.");
    }
    return name;
  }

  /**
   * OFS: Test mkdir on volume, bucket and dir that doesn't exist.
   */
  @Test
  public void testMkdirOnNonExistentVolumeBucketDir() throws Exception {
    String volumeNameLocal = getRandomNonExistVolumeName();
    String bucketNameLocal = "bucket-" + RandomStringUtils.randomNumeric(5);
    Path root = new Path("/" + volumeNameLocal + "/" + bucketNameLocal);
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // Check volume and bucket existence, they should both be created.
    OzoneVolume ozoneVolume = objectStore.getVolume(volumeNameLocal);
    OzoneBucket ozoneBucket = ozoneVolume.getBucket(bucketNameLocal);
    OFSPath ofsPathDir1 = new OFSPath(dir12);
    String key = ofsPathDir1.getKeyName() + "/";
    OzoneKeyDetails ozoneKeyDetails = ozoneBucket.getKey(key);
    Assert.assertEquals(key, ozoneKeyDetails.getName());

    // Verify that directories are created.
    FileStatus[] fileStatuses = ofs.listStatus(root);
    Assert.assertEquals(
        fileStatuses[0].getPath().toUri().getPath(), dir1.toString());
    Assert.assertEquals(
        fileStatuses[1].getPath().toUri().getPath(), dir2.toString());

    fileStatuses = ofs.listStatus(dir1);
    Assert.assertEquals(
        fileStatuses[0].getPath().toUri().getPath(), dir12.toString());
    fileStatuses = ofs.listStatus(dir12);
    Assert.assertEquals(fileStatuses.length, 0);
    fileStatuses = ofs.listStatus(dir2);
    Assert.assertEquals(fileStatuses.length, 0);

    // Cleanup
    fs.delete(dir2, true);
    fs.delete(dir1, true);
    ozoneVolume.deleteBucket(bucketNameLocal);
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test mkdir on a volume and bucket that doesn't exist.
   */
  @Test
  public void testMkdirNonExistentVolumeBucket() throws Exception {
    String volumeNameLocal = getRandomNonExistVolumeName();
    String bucketNameLocal = "bucket-" + RandomStringUtils.randomNumeric(5);
    Path newVolBucket = new Path(
        "/" + volumeNameLocal + "/" + bucketNameLocal);
    fs.mkdirs(newVolBucket);

    // Verify with listVolumes and listBuckets
    Iterator<? extends OzoneVolume> iterVol =
        objectStore.listVolumesByUser(null, volumeNameLocal, null);
    OzoneVolume ozoneVolume = iterVol.next();
    Assert.assertNotNull(ozoneVolume);
    Assert.assertEquals(volumeNameLocal, ozoneVolume.getName());

    Iterator<? extends OzoneBucket> iterBuc =
        ozoneVolume.listBuckets("bucket-");
    OzoneBucket ozoneBucket = iterBuc.next();
    Assert.assertNotNull(ozoneBucket);
    Assert.assertEquals(bucketNameLocal, ozoneBucket.getName());

    // TODO: Use listStatus to check volume and bucket creation in HDDS-2928.

    // Cleanup
    ozoneVolume.deleteBucket(bucketNameLocal);
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test mkdir on a volume that doesn't exist.
   */
  @Test
  public void testMkdirNonExistentVolume() throws Exception {
    String volumeNameLocal = getRandomNonExistVolumeName();
    Path newVolume = new Path("/" + volumeNameLocal);
    fs.mkdirs(newVolume);

    // Verify with listVolumes and listBuckets
    Iterator<? extends OzoneVolume> iterVol =
        objectStore.listVolumesByUser(null, volumeNameLocal, null);
    OzoneVolume ozoneVolume = iterVol.next();
    Assert.assertNotNull(ozoneVolume);
    Assert.assertEquals(volumeNameLocal, ozoneVolume.getName());

    // TODO: Use listStatus to check volume and bucket creation in HDDS-2928.

    // Cleanup
    objectStore.deleteVolume(volumeNameLocal);
  }

  /**
   * OFS: Test getFileStatus on root.
   */
  @Test
  public void testGetFileStatusRoot() throws Exception {
    Path root = new Path("/");
    FileStatus fileStatus = fs.getFileStatus(root);
    Assert.assertNotNull(fileStatus);
    Assert.assertEquals(new Path(rootPath), fileStatus.getPath());
    Assert.assertTrue(fileStatus.isDirectory());
    Assert.assertEquals(FsPermission.getDirDefault(),
        fileStatus.getPermission());
  }

  /**
   * Test listStatus operation in a bucket.
   */
  @Test
  public void testListStatusInBucket() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    fs.mkdirs(dir2);

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    FileStatus[] fileStatuses = ofs.listStatus(root);
    Assert.assertEquals(
        "FileStatus should return only the immediate children",
        2, fileStatuses.length);

    // Verify that dir12 is not included in the result of the listStatus on root
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    Assert.assertNotEquals(fileStatus1, dir12.toString());
    Assert.assertNotEquals(fileStatus2, dir12.toString());
  }

  /**
   * Tests listStatus operation on root directory.
   */
  @Test
  public void testListStatusOnLargeDirectory() throws Exception {
    Path root = new Path("/" + volumeName + "/" + bucketName);
    Set<String> paths = new TreeSet<>();
    int numDirs = LISTING_PAGE_SIZE + LISTING_PAGE_SIZE / 2;
    for(int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
    }

    FileStatus[] fileStatuses = ofs.listStatus(root);
    Assert.assertEquals(
        "Total directories listed do not match the existing directories",
        numDirs, fileStatuses.length);

    for (int i=0; i < numDirs; i++) {
      Assert.assertTrue(paths.contains(fileStatuses[i].getPath().getName()));
    }

    // Cleanup
    for(int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      fs.delete(p, true);
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
    // ListStatus on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatus.
    Path dir1 = new Path(bucketPath, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(bucketPath, "dir2");
    fs.mkdirs(dir111);
    fs.mkdirs(dir12);
    ContractTestUtils.touch(fs, file121);
    fs.mkdirs(dir2);

    FileStatus[] fileStatuses = ofs.listStatus(dir1);
    Assert.assertEquals(
        "FileStatus should return only the immediate children",
        2, fileStatuses.length);

    // Verify that the two children of /dir1 returned by listStatus operation
    // are /dir1/dir11 and /dir1/dir12.
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    Assert.assertTrue(fileStatus1.equals(dir11.toString()) ||
        fileStatus1.equals(dir12.toString()));
    Assert.assertTrue(fileStatus2.equals(dir11.toString()) ||
        fileStatus2.equals(dir12.toString()));

    // Cleanup
    fs.delete(dir2, true);
    fs.delete(dir1, true);
  }

  @Test
  public void testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved()
      throws Exception {
    Path source = new Path(bucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(bucketPath, "target");
    Path leafInTarget = new Path(target, "leaf");

    fs.mkdirs(source);
    fs.mkdirs(target);
    fs.mkdirs(leafInsideInterimPath);

    Assert.assertTrue(fs.rename(leafInsideInterimPath, leafInTarget));

    // after rename listStatus for interimPath should succeed and
    // interimPath should have no children
    FileStatus[] statuses = fs.listStatus(interimPath);
    Assert.assertNotNull("liststatus returns a null array", statuses);
    Assert.assertEquals("Statuses array is not empty", 0, statuses.length);
    FileStatus fileStatus = fs.getFileStatus(interimPath);
    Assert.assertEquals("FileStatus does not point to interimPath",
        interimPath.getName(), fileStatus.getPath().getName());

    // Cleanup
    fs.delete(target, true);
    fs.delete(source, true);
  }

  /**
   * OFS: Try to rename a key to a different bucket. The attempt should fail.
   */
  @Test
  public void testRenameToDifferentBucket() throws IOException {
    Path source = new Path(bucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(bucketPath, "target");

    fs.mkdirs(source);
    fs.mkdirs(target);
    fs.mkdirs(leafInsideInterimPath);

    // Attempt to rename the key to a different bucket
    Path bucket2 = new Path(OZONE_URI_DELIMITER + volumeName +
        OZONE_URI_DELIMITER + bucketName + "test");
    Path leafInTargetInAnotherBucket = new Path(bucket2, "leaf");
    try {
      fs.rename(leafInsideInterimPath, leafInTargetInAnotherBucket);
      Assert.fail(
          "Should have thrown exception when renaming to a different bucket");
    } catch (IOException ignored) {
      // Test passed. Exception thrown as expected.
    }

    // Cleanup
    fs.delete(target, true);
    fs.delete(source, true);
  }

  private OzoneKeyDetails getKey(Path keyPath, boolean isDirectory)
      throws IOException {
    String key = ofs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + OZONE_URI_DELIMITER;
    }
    OFSPath ofsPath = new OFSPath(key);
    String keyInBucket = ofsPath.getKeyName();
    return cluster.getClient().getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(keyInBucket);
  }

  private void assertKeyNotFoundException(IOException ex) {
    GenericTestUtils.assertExceptionContains("KEY_NOT_FOUND", ex);
  }

  /**
   * Helper function for testListStatusRootAndVolume*.
   * Each call creates one volume, one bucket under that volume,
   * two dir under that bucket, one subdir under one of the dirs,
   * and one file under the subdir.
   */
  private Path createRandomVolumeBucketWithDirs() throws IOException {
    String volume1 = getRandomNonExistVolumeName();
    String bucket1 = "bucket-" + RandomStringUtils.randomNumeric(5);
    Path bucketPath1 = new Path(OZONE_URI_DELIMITER + volume1 +
        OZONE_URI_DELIMITER + bucket1);

    Path dir1 = new Path(bucketPath1, "dir1");
    fs.mkdirs(dir1);  // Intentionally creating this "in-the-middle" dir key
    Path subdir1 = new Path(dir1, "subdir1");
    fs.mkdirs(subdir1);
    Path dir2 = new Path(bucketPath1, "dir2");
    fs.mkdirs(dir2);

    try (FSDataOutputStream stream =
        ofs.create(new Path(dir2, "file1"))) {
      stream.write(1);
    }

    return bucketPath1;
  }

  private void teardownVolumeBucketWithDir(Path bucketPath1)
      throws IOException {
    fs.delete(new Path(bucketPath1, "dir1"), true);
    fs.delete(new Path(bucketPath1, "dir2"), true);
    OFSPath ofsPath = new OFSPath(bucketPath1);
    OzoneVolume volume = objectStore.getVolume(ofsPath.getVolumeName());
    volume.deleteBucket(ofsPath.getBucketName());
    objectStore.deleteVolume(ofsPath.getVolumeName());
  }

  /**
   * OFS: Test non-recursive listStatus on root and volume.
   */
  @Test
  public void testListStatusRootAndVolumeNonRecursive() throws Exception {
    // Get owner and group of the user running this test
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    final String ownerShort = ugi.getShortUserName();
    final String group = ugi.getPrimaryGroupName();

    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    Path bucketPath2 = createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    FileStatus[] fileStatusBucket = ofs.listStatus(bucketPath1);
    Assert.assertEquals(2, fileStatusBucket.length);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1).getVolumeName());
    FileStatus[] fileStatusVolume = ofs.listStatus(volume);
    Assert.assertEquals(1, fileStatusVolume.length);
    Assert.assertEquals(ownerShort, fileStatusVolume[0].getOwner());
    Assert.assertEquals(group, fileStatusVolume[0].getGroup());
    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    FileStatus[] fileStatusRoot = ofs.listStatus(root);
    // Default volume "s3v" is created by OM during start up.
    Assert.assertEquals(2 + 1, fileStatusRoot.length);
    for (FileStatus fileStatus : fileStatusRoot) {
      Assert.assertEquals(ownerShort, fileStatus.getOwner());
      Assert.assertEquals(group, fileStatus.getGroup());
    }
    // Cleanup
    teardownVolumeBucketWithDir(bucketPath2);
    teardownVolumeBucketWithDir(bucketPath1);
  }

  /**
   * Helper function to do FileSystem#listStatus recursively.
   * Simulate what FsShell does, using DFS.
   */
  private void listStatusRecursiveHelper(Path curPath, List<FileStatus> result)
      throws IOException {
    FileStatus[] startList = ofs.listStatus(curPath);
    for (FileStatus fileStatus : startList) {
      result.add(fileStatus);
      if (fileStatus.isDirectory()) {
        Path nextPath = fileStatus.getPath();
        listStatusRecursiveHelper(nextPath, result);
      }
    }
  }

  /**
   * Helper function to call listStatus in adapter implementation.
   */
  private List<FileStatus> callAdapterListStatus(String pathStr,
      boolean recursive, String startPath, long numEntries) throws IOException {
    return adapter.listStatus(pathStr, recursive, startPath, numEntries,
        ofs.getUri(), ofs.getWorkingDirectory(), ofs.getUsername())
        .stream().map(ofs::convertFileStatus).collect(Collectors.toList());
  }

  /**
   * Helper function to compare recursive listStatus results from adapter
   * and (simulated) FileSystem.
   */
  private void listStatusCheckHelper(Path path) throws IOException {
    // Get recursive listStatus result directly from adapter impl
    List<FileStatus> statusesFromAdapter = callAdapterListStatus(
        path.toString(), true, "", 1000);
    // Get recursive listStatus result with FileSystem API by simulating FsShell
    List<FileStatus> statusesFromFS = new ArrayList<>();
    listStatusRecursiveHelper(path, statusesFromFS);
    // Compare. The results would be in the same order due to assumptions:
    // 1. They are both using DFS internally;
    // 2. They both return ordered results.
    Assert.assertEquals(statusesFromAdapter.size(), statusesFromFS.size());
    final int n = statusesFromFS.size();
    for (int i = 0; i < n; i++) {
      FileStatus statusFromAdapter = statusesFromAdapter.get(i);
      FileStatus statusFromFS = statusesFromFS.get(i);
      Assert.assertEquals(statusFromAdapter.getPath(), statusFromFS.getPath());
      Assert.assertEquals(statusFromAdapter.getLen(), statusFromFS.getLen());
      Assert.assertEquals(statusFromAdapter.isDirectory(),
          statusFromFS.isDirectory());
      Assert.assertEquals(statusFromAdapter.getModificationTime(),
          statusFromFS.getModificationTime());
    }
  }

  /**
   * OFS: Test recursive listStatus on root and volume.
   */
  @Test
  public void testListStatusRootAndVolumeRecursive() throws IOException {
    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    Path bucketPath2 = createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    listStatusCheckHelper(bucketPath1);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1).getVolumeName());
    listStatusCheckHelper(volume);
    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    listStatusCheckHelper(root);
    // Cleanup
    teardownVolumeBucketWithDir(bucketPath2);
    teardownVolumeBucketWithDir(bucketPath1);
  }

  /**
   * Helper function. FileSystem#listStatus on steroid:
   * Supports recursion, start path and custom listing page size (numEntries).
   * @param f Given path
   * @param recursive List contents inside subdirectories
   * @param startPath Starting path of the batch
   * @param numEntries Max number of entries in result
   * @return Array of the statuses of the files/directories in the given path
   * @throws IOException See specific implementation
   */
  private FileStatus[] customListStatus(Path f, boolean recursive,
      String startPath, int numEntries) throws IOException {
    Assert.assertTrue(numEntries > 0);
    LinkedList<FileStatus> statuses = new LinkedList<>();
    List<FileStatus> tmpStatusList;
    do {
      tmpStatusList = callAdapterListStatus(f.toString(), recursive,
          startPath, numEntries - statuses.size());
      if (!tmpStatusList.isEmpty()) {
        statuses.addAll(tmpStatusList);
        startPath = statuses.getLast().getPath().toString();
      }
    } while (tmpStatusList.size() == numEntries &&
        statuses.size() < numEntries);
    return statuses.toArray(new FileStatus[0]);
  }

  @Test
  public void testListStatusRootAndVolumeContinuation() throws IOException {
    Path[] paths = new Path[5];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = createRandomVolumeBucketWithDirs();
    }
    // Similar to recursive option, we can't test continuation directly with
    // FileSystem because we can't change LISTING_PAGE_SIZE. Use adapter instead

    // numEntries > 5
    FileStatus[] fileStatusesOver = customListStatus(new Path("/"),
        false, "", 8);
    // There are only 5 volumes
    // Default volume "s3v" is created during startup.
    Assert.assertEquals(5 + 1, fileStatusesOver.length);

    // numEntries = 5
    FileStatus[] fileStatusesExact = customListStatus(new Path("/"),
        false, "", 5);
    Assert.assertEquals(5, fileStatusesExact.length);

    // numEntries < 5
    FileStatus[] fileStatusesLimit1 = customListStatus(new Path("/"),
        false, "", 3);
    // Should only return 3 volumes even though there are more than that due to
    // the specified limit
    Assert.assertEquals(3, fileStatusesLimit1.length);

    // Get the last entry in the list as startPath
    String nextStartPath =
        fileStatusesLimit1[fileStatusesLimit1.length - 1].getPath().toString();
    FileStatus[] fileStatusesLimit2 = customListStatus(new Path("/"),
        false, nextStartPath, 3);
    // Note: at the time of writing this test, OmMetadataManagerImpl#listVolumes
    //  excludes startVolume (startPath) from the result. Might change.
    Assert.assertEquals(fileStatusesOver.length,
        fileStatusesLimit1.length + fileStatusesLimit2.length);

    // Cleanup
    for (Path path : paths) {
      teardownVolumeBucketWithDir(path);
    }
  }

   /*
   * OFS: Test /tmp mount behavior.
   */
  @Test
  public void testTempMount() throws Exception {
    // Prep
    // Use ClientProtocol to pass in volume ACL, ObjectStore won't do it
    ClientProtocol proxy = objectStore.getClientProxy();
    // Get default acl rights for user
    OzoneAclConfig aclConfig = conf.getObject(OzoneAclConfig.class);
    ACLType userRights = aclConfig.getUserDefaultRights();
    // Construct ACL for world access
    OzoneAcl aclWorldAccess = new OzoneAcl(ACLIdentityType.WORLD, "",
        userRights, ACCESS);
    // Construct VolumeArgs
    VolumeArgs volumeArgs = new VolumeArgs.Builder()
        .setAcls(Collections.singletonList(aclWorldAccess))
        .setQuotaInNamespace(1000)
        .setQuotaInBytes(Long.MAX_VALUE).build();
    // Sanity check
    Assert.assertNull(volumeArgs.getOwner());
    Assert.assertNull(volumeArgs.getAdmin());
    Assert.assertEquals(Long.MAX_VALUE, volumeArgs.getQuotaInBytes());
    Assert.assertEquals(1000, volumeArgs.getQuotaInNamespace());
    Assert.assertEquals(0, volumeArgs.getMetadata().size());
    Assert.assertEquals(1, volumeArgs.getAcls().size());
    // Create volume "tmp" with world access. allow non-admin to create buckets
    proxy.createVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME, volumeArgs);

    OzoneVolume vol = objectStore.getVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
    Assert.assertNotNull(vol);

    // Begin test
    String hashedUsername = OFSPath.getTempMountBucketNameOfCurrentUser();

    // Expect failure since temp bucket for current user is not created yet
    try {
      vol.getBucket(hashedUsername);
    } catch (OMException ex) {
      // Expect BUCKET_NOT_FOUND
      if (!ex.getResult().equals(BUCKET_NOT_FOUND)) {
        Assert.fail("Temp bucket for current user shouldn't have been created");
      }
    }

    // Write under /tmp/, OFS will create the temp bucket if not exist
    Path dir1 = new Path("/tmp/dir1");
    fs.mkdirs(dir1);

    try (FSDataOutputStream stream = ofs.create(new Path("/tmp/dir1/file1"))) {
      stream.write(1);
    }

    // Verify temp bucket creation
    OzoneBucket bucket = vol.getBucket(hashedUsername);
    Assert.assertNotNull(bucket);
    // Verify dir1 creation
    FileStatus[] fileStatuses = fs.listStatus(new Path("/tmp/"));
    Assert.assertEquals(1, fileStatuses.length);
    Assert.assertEquals(
        "/tmp/dir1", fileStatuses[0].getPath().toUri().getPath());
    // Verify file1 creation
    FileStatus[] fileStatusesInDir1 = fs.listStatus(dir1);
    Assert.assertEquals(1, fileStatusesInDir1.length);
    Assert.assertEquals("/tmp/dir1/file1",
        fileStatusesInDir1[0].getPath().toUri().getPath());

    // Cleanup
    fs.delete(dir1, true);
    vol.deleteBucket(hashedUsername);
    proxy.deleteVolume(OFSPath.OFS_MOUNT_TMP_VOLUMENAME);
  }

  /**
   * Helper function. Check Ozone volume existence.
   * @param volumeStr Name of the volume
   * @return true if volume exists, false if not
   */
  private boolean volumeExist(String volumeStr) throws IOException {
    try {
      objectStore.getVolume(volumeStr);
    } catch (OMException ex) {
      if (ex.getResult() == VOLUME_NOT_FOUND) {
        return false;
      } else {
        throw ex;
      }
    }
    return true;
  }

  /**
   * Helper function. Delete a path non-recursively and expect failure.
   * @param f Path to delete.
   * @throws IOException
   */
  private void deleteNonRecursivelyAndFail(Path f) throws IOException {
    try {
      fs.delete(f, false);
      Assert.fail("Should have thrown PathIsNotEmptyDirectoryException!");
    } catch (PathIsNotEmptyDirectoryException ignored) {
    }
  }

  @Test
  public void testDeleteEmptyVolume() throws IOException {
    // Create volume
    String volumeStr1 = getRandomNonExistVolumeName();
    Path volumePath1 = new Path(OZONE_URI_DELIMITER + volumeStr1);
    fs.mkdirs(volumePath1);
    // Check volume creation
    OzoneVolume volume1 = objectStore.getVolume(volumeStr1);
    Assert.assertEquals(volumeStr1, volume1.getName());
    // Delete empty volume non-recursively
    Assert.assertTrue(fs.delete(volumePath1, false));
    // Verify the volume is deleted
    Assert.assertFalse(volumeStr1 + " should have been deleted!",
        volumeExist(volumeStr1));
  }

  @Test
  public void testDeleteVolumeAndBucket() throws IOException {
    // Create volume and bucket
    String volumeStr2 = getRandomNonExistVolumeName();
    Path volumePath2 = new Path(OZONE_URI_DELIMITER + volumeStr2);
    String bucketStr2 = "bucket2";
    Path bucketPath2 = new Path(volumePath2, bucketStr2);
    fs.mkdirs(bucketPath2);
    // Check volume and bucket creation
    OzoneVolume volume2 = objectStore.getVolume(volumeStr2);
    Assert.assertEquals(volumeStr2, volume2.getName());
    OzoneBucket bucket2 = volume2.getBucket(bucketStr2);
    Assert.assertEquals(bucketStr2, bucket2.getName());
    // Delete volume non-recursively should fail since it is not empty
    deleteNonRecursivelyAndFail(volumePath2);
    // Delete bucket first, then volume
    Assert.assertTrue(fs.delete(bucketPath2, false));
    Assert.assertTrue(fs.delete(volumePath2, false));
    // Verify the volume is deleted
    Assert.assertFalse(volumeExist(volumeStr2));
  }

  @Test
  public void testDeleteVolumeBucketAndKey() throws IOException {
    // Create test volume, bucket and key
    String volumeStr3 = getRandomNonExistVolumeName();
    Path volumePath3 = new Path(OZONE_URI_DELIMITER + volumeStr3);
    String bucketStr3 = "bucket3";
    Path bucketPath3 = new Path(volumePath3, bucketStr3);
    String dirStr3 = "dir3";
    Path dirPath3 = new Path(bucketPath3, dirStr3);
    fs.mkdirs(dirPath3);
    // Delete volume or bucket non-recursively, should fail
    deleteNonRecursivelyAndFail(volumePath3);
    deleteNonRecursivelyAndFail(bucketPath3);
    // Delete key first, then bucket, then volume
    Assert.assertTrue(fs.delete(dirPath3, false));
    Assert.assertTrue(fs.delete(bucketPath3, false));
    Assert.assertTrue(fs.delete(volumePath3, false));
    // Verify the volume is deleted
    Assert.assertFalse(volumeExist(volumeStr3));

    // Test recursively delete volume
    // Create test volume, bucket and key
    fs.mkdirs(dirPath3);
    // Delete volume recursively
    Assert.assertTrue(fs.delete(volumePath3, true));
    // Verify the volume is deleted
    Assert.assertFalse(volumeExist(volumeStr3));
  }

  @Test
  public void testFailToDeleteRoot() throws IOException {
    // rm root should always fail for OFS
    Assert.assertFalse(fs.delete(new Path("/"), false));
    Assert.assertFalse(fs.delete(new Path("/"), true));
  }

  /**
   * Helper function for testGetTrashRoots() for checking the first element
   * in the FileStatus Collection.
   * @param expected Expected path String
   * @param res Collection of FileStatus from getTrashRoots()
   */
  private void checkFirstFileStatusPath(String expected,
      Collection<FileStatus> res) {
    Optional<FileStatus> optional = res.stream().findFirst();
    Assert.assertTrue(optional.isPresent());
    Assert.assertEquals(expected, optional.get().getPath().toUri().getPath());
  }

  /**
   * Helper function for testGetTrashRoots() for checking all owner field in
   * FileStatuses in the Collection.
   * @param expectedSize Expected size of the FileStatus Collection
   * @param expectedOwner Expected owner String
   * @param res Collection of FileStatus from getTrashRoots()
   */
  private void checkFileStatusOwner(int expectedSize, String expectedOwner,
      Collection<FileStatus> res) {
    Assert.assertEquals(expectedSize, res.size());
    res.forEach(e -> Assert.assertEquals(expectedOwner, e.getOwner()));
  }

  /**
   * Test getTrashRoots() in OFS. Different from the existing test for o3fs.
   */
  @Test
  public void testGetTrashRoots() throws IOException {
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    OzoneVolume volume1 = objectStore.getVolume(volumeName);
    String prevOwner = volume1.getOwner();
    // Set owner of the volume to current user, so it will show up in vol list
    Assert.assertTrue(volume1.setOwner(username));

    Path trashRoot1 = new Path(bucketPath, TRASH_PREFIX);
    Path user1Trash1 = new Path(trashRoot1, username);
    // When user trash dir hasn't been created
    Assert.assertEquals(0, fs.getTrashRoots(false).size());
    Assert.assertEquals(0, fs.getTrashRoots(true).size());
    // Let's create our first user1 (current user) trash dir.
    fs.mkdirs(user1Trash1);
    // Results should be getTrashRoots(false)=1, gTR(true)=1
    Collection<FileStatus> res = fs.getTrashRoots(false);
    Assert.assertEquals(1, res.size());
    checkFirstFileStatusPath(user1Trash1.toString(), res);
    res = fs.getTrashRoots(true);
    Assert.assertEquals(1, res.size());
    checkFirstFileStatusPath(user1Trash1.toString(), res);

    // Create one more trash for user2 in the same bucket
    Path user2Trash1 = new Path(trashRoot1, "testuser2");
    fs.mkdirs(user2Trash1);
    // Results should be getTrashRoots(false)=1, gTR(true)=2
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    Assert.assertEquals(2, fs.getTrashRoots(true).size());

    // Create a new bucket in the same volume
    final String bucketName2 = "trashroottest2";
    volume1.createBucket(bucketName2);
    Path bucketPath2 = new Path(volumePath, bucketName2);
    Path trashRoot2 = new Path(bucketPath2, TRASH_PREFIX);
    Path user1Trash2 = new Path(trashRoot2, username);
    // Create a file at the trash location, it shouldn't be recognized as trash
    try (FSDataOutputStream out1 = fs.create(user1Trash2)) {
      out1.write(123);
    }
    // Results should still be getTrashRoots(false)=1, gTR(true)=2
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    checkFirstFileStatusPath(user1Trash1.toString(), res);
    Assert.assertEquals(2, fs.getTrashRoots(true).size());
    // Remove the file and create a dir instead. It should be recognized now
    fs.delete(user1Trash2, false);
    fs.mkdirs(user1Trash2);
    // Results should now be getTrashRoots(false)=2, gTR(true)=3
    checkFileStatusOwner(2, username, fs.getTrashRoots(false));
    Assert.assertEquals(3, fs.getTrashRoots(true).size());

    // Create a new volume and a new bucket
    OzoneBucket bucket3 = TestDataUtil.createVolumeAndBucket(cluster);
    OzoneVolume volume3 = objectStore.getVolume(bucket3.getVolumeName());
    // Need to setOwner to current test user so it has permission to list vols
    volume3.setOwner(username);
    Path volumePath3 = new Path(OZONE_URI_DELIMITER, bucket3.getVolumeName());
    Path bucketPath3 = new Path(volumePath3, bucket3.getName());
    Path trashRoot3 = new Path(bucketPath3, TRASH_PREFIX);
    Path user1Trash3 = new Path(trashRoot3, username);
    // Results should be getTrashRoots(false)=3, gTR(true)=4
    fs.mkdirs(user1Trash3);
    checkFileStatusOwner(3, username, fs.getTrashRoots(false));
    Assert.assertEquals(4, fs.getTrashRoots(true).size());
    // One more user
    Path user3Trash1 = new Path(trashRoot3, "testuser3");
    fs.mkdirs(user3Trash1);
    // Results should be getTrashRoots(false)=3, gTR(true)=5
    checkFileStatusOwner(3, username, fs.getTrashRoots(false));
    Assert.assertEquals(5, fs.getTrashRoots(true).size());

    // Clean up, and check while doing so
    fs.delete(trashRoot3, true);
    checkFileStatusOwner(2, username, fs.getTrashRoots(false));
    Assert.assertEquals(3, fs.getTrashRoots(true).size());
    fs.delete(trashRoot2, true);
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    Assert.assertEquals(2, fs.getTrashRoots(true).size());
    fs.delete(user2Trash1, true);
    checkFileStatusOwner(1, username, fs.getTrashRoots(false));
    Assert.assertEquals(1, fs.getTrashRoots(true).size());

    volume3.deleteBucket(bucket3.getName());
    objectStore.deleteVolume(volume3.getName());
    volume1.deleteBucket(bucketName2);

    fs.delete(user1Trash1, true);
    Assert.assertEquals(0, fs.getTrashRoots(false).size());
    Assert.assertEquals(0, fs.getTrashRoots(true).size());
    fs.delete(trashRoot1, true);
    // Restore owner
    Assert.assertTrue(volume1.setOwner(prevOwner));
  }

  /**
   * Check that  files are moved to trash since it is enabled by
   * fs.rename(src, dst, options).
   */
  @Test
  public void testRenameToTrashEnabled() throws IOException {
    // Create a file
    String testKeyName = "testKey2";
    Path path = new Path(bucketPath, testKeyName);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }

    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(path);

    // Construct paths
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path trashRoot = new Path(bucketPath, TRASH_PREFIX);
    Path userTrash = new Path(trashRoot, username);
    Path userTrashCurrent = new Path(userTrash, "Current");
    String key = path.toString().substring(1);
    Path trashPath = new Path(userTrashCurrent, key);
    // Trash Current directory should still have been created.
    Assert.assertTrue(ofs.exists(userTrashCurrent));
    // Check under trash, the key should be present
    Assert.assertTrue(ofs.exists(trashPath));

    // Cleanup
    ofs.delete(trashRoot, true);
  }

  @Test
  public void testFileDelete() throws Exception {
    Path grandparent = new Path(bucketPath, "testBatchDelete");
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

    assertTrue(fs.listStatus(grandparent).length == 1);
    assertTrue(fs.listStatus(parent).length == 9);
    assertTrue(fs.listStatus(childFolder).length == 8);

    Boolean successResult = fs.delete(grandparent, true);
    assertTrue(successResult);
    assertTrue(!ofs.exists(grandparent));
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      // Make sure all keys under testBatchDelete/parent should be deleted
      assertTrue(!ofs.exists(childFile));

      // Test to recursively delete child folder, make sure all keys under
      // testBatchDelete/parent/childFolder should be deleted.
      Path childFolderFile = new Path(childFolder, "child" + i);
      assertTrue(!ofs.exists(childFolderFile));
    }
    // Will get: WARN  ozone.BasicOzoneFileSystem delete: Path does not exist.
    // This will return false.
    Boolean falseResult = fs.delete(parent, true);
    assertFalse(falseResult);
  }

  /**
   * 1.Move a Key to Trash
   * 2.Verify that the key gets deleted by the trash emptier.
   * @throws Exception
   */
  @Test
  public void testTrash() throws Exception {
    String testKeyName = "keyToBeDeleted";
    Path path = new Path(bucketPath, testKeyName);
    try (FSDataOutputStream stream = fs.create(path)) {
      stream.write(1);
    }
    Assert.assertTrue(trash.getConf().getClass(
        "fs.trash.classname", TrashPolicy.class).
        isAssignableFrom(TrashPolicyOzone.class));

    // Call moveToTrash. We can't call protected fs.rename() directly
    trash.moveToTrash(path);

    // Construct paths
    String username = UserGroupInformation.getCurrentUser().getShortUserName();
    Path trashRoot = new Path(bucketPath, TRASH_PREFIX);
    Path userTrash = new Path(trashRoot, username);
    Path userTrashCurrent = new Path(userTrash, "Current");
    String key = path.toString().substring(1);
    Path trashPath = new Path(userTrashCurrent, key);

    // Wait until the TrashEmptier purges the key
    GenericTestUtils.waitFor(()-> {
      try {
        return !ofs.exists(trashPath);
      } catch (IOException e) {
        LOG.error("Delete from Trash Failed", e);
        Assert.fail("Delete from Trash Failed");
        return false;
      }
    }, 1000, 180000);

    // Cleanup
    ofs.delete(trashRoot, true);

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

  private void checkInvalidPath(Path path) throws Exception {
    LambdaTestUtils.intercept(InvalidPathException.class, "Invalid path Name",
        () -> fs.create(path, false));
  }
}
