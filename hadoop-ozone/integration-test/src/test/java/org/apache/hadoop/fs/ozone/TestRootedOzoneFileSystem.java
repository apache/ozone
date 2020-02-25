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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.hadoop.fs.ozone.Constants.LISTING_PAGE_SIZE;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

/**
 * Ozone file system tests that are not covered by contract tests.
 * TODO: Refactor this and TestOzoneFileSystem later to reduce code duplication.
 */
public class TestRootedOzoneFileSystem {

  @Rule
  public Timeout globalTimeout = new Timeout(300_000);

  private static MiniOzoneCluster cluster = null;
  private static FileSystem fs;
  private static RootedOzoneFileSystem ofs;
  private static ObjectStore objectStore;

  private String volumeName;
  private String bucketName;
  // Store path commonly used by tests that test functionality within a bucket
  private Path testBucketPath;
  private String rootPath;

  @Before
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = cluster.getClient().getObjectStore();

    // create a volume and a bucket to be used by RootedOzoneFileSystem (OFS)
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster);
    volumeName = bucket.getVolumeName();
    bucketName = bucket.getName();
    String testBucketStr =
        OZONE_URI_DELIMITER + volumeName + OZONE_URI_DELIMITER + bucketName;
    testBucketPath = new Path(testBucketStr);

    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));

    // Set the fs.defaultFS and start the filesystem
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // Note: FileSystem#loadFileSystems won't load OFS class due to META-INF
    //  hence this workaround.
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
    fs = FileSystem.get(conf);
    ofs = (RootedOzoneFileSystem) fs;
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  @Test
  public void testOzoneFsServiceLoader() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Note: FileSystem#loadFileSystems won't load OFS class due to META-INF
    //  hence this workaround.
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.RootedOzoneFileSystem");
    Assert.assertEquals(
        FileSystem.getFileSystemClass(OzoneConsts.OZONE_OFS_URI_SCHEME, conf),
        RootedOzoneFileSystem.class);
  }

  @Test
  public void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path(testBucketPath,
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
  }

  @Test
  public void testDeleteCreatesFakeParentDir() throws Exception {
    Path grandparent = new Path(testBucketPath,
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
    Path parent = new Path(testBucketPath, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");

    FileStatus[] fileStatuses = ofs.listStatus(testBucketPath);
    Assert.assertEquals("Should be empty", 0, fileStatuses.length);

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = ofs.listStatus(testBucketPath);
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
    Path dir1 = new Path(testBucketPath, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(testBucketPath, "dir2");
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
  }

  @Test
  public void testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved()
      throws Exception {
    Path source = new Path(testBucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(testBucketPath, "target");
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
  }

  /**
   * OFS: Try to rename a key to a different bucket. The attempt should fail.
   */
  @Test
  public void testRenameToDifferentBucket() throws IOException {
    Path source = new Path(testBucketPath, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(testBucketPath, "target");

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
   */
  private Path createRandomVolumeBucketWithDirs() throws IOException {
    String volume1 = getRandomNonExistVolumeName();
    String bucket1 = "bucket-" + RandomStringUtils.randomNumeric(5);
    Path bucketPath1 = new Path(
        OZONE_URI_DELIMITER + volume1 + OZONE_URI_DELIMITER + bucket1);

    Path dir1 = new Path(bucketPath1, "dir1");
    fs.mkdirs(dir1);  // Intentionally creating this "in-the-middle" dir key
    Path subdir1 = new Path(dir1, "subdir1");
    fs.mkdirs(subdir1);
    Path dir2 = new Path(bucketPath1, "dir2");
    fs.mkdirs(dir2);

    return bucketPath1;
  }

  /**
   * OFS: Test non-recursive listStatus on root and volume.
   */
  @Test
  public void testListStatusRootAndVolumeNonRecursive() throws Exception {
    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    FileStatus[] fileStatusBucket = ofs.listStatus(bucketPath1);
    Assert.assertEquals(2, fileStatusBucket.length);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1).getVolumeName());
    FileStatus[] fileStatusVolume = ofs.listStatus(volume);
    Assert.assertEquals(1, fileStatusVolume.length);
    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    FileStatus[] fileStatusRoot = ofs.listStatus(root);
    Assert.assertEquals(2, fileStatusRoot.length);
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
   * Helper function to call adapter impl for listStatus (with recursive).
   */
  private List<FileStatus> listStatusCallAdapterHelper(String pathStr)
      throws IOException {
    // FileSystem interface does not support recursive listStatus, use adapter
    BasicRootedOzoneClientAdapterImpl adapter =
        (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();
    return adapter.listStatus(pathStr, true, "", 1000,
        ofs.getUri(), ofs.getWorkingDirectory(), ofs.getUsername())
        .stream().map(ofs::convertFileStatus).collect(Collectors.toList());
  }

  /**
   * Helper function to compare recursive listStatus results from adapter
   * and (simulated) FileSystem.
   */
  private void listStatusCheckHelper(Path path) throws IOException {
    // Get recursive listStatus result directly from adapter impl
    List<FileStatus> statusesFromAdapter =
        listStatusCallAdapterHelper(path.toString());
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
      // TODO: When HDDS-3054 is in, uncomment the lines below.
      //  As of now the modification time almost certainly won't match.
//      Assert.assertEquals(statusFromAdapter.getModificationTime(),
//          statusFromFS.getModificationTime());
    }
  }

  /**
   * OFS: Test recursive listStatus on root and volume.
   */
  @Test
  public void testListStatusRootAndVolumeRecursive() throws Exception {
    Path bucketPath1 = createRandomVolumeBucketWithDirs();
    createRandomVolumeBucketWithDirs();
    // listStatus("/volume/bucket")
    listStatusCheckHelper(bucketPath1);
    // listStatus("/volume")
    Path volume = new Path(
        OZONE_URI_DELIMITER + new OFSPath(bucketPath1).getVolumeName());
    listStatusCheckHelper(volume);
    // listStatus("/")
    Path root = new Path(OZONE_URI_DELIMITER);
    listStatusCheckHelper(root);
  }

}
