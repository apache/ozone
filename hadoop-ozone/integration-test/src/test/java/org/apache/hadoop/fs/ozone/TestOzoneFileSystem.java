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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.io.IOUtils;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
public class TestOzoneFileSystem {

  /**
    * Set a timeout for each test.
    */
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
    setupOzoneFileSystem();

    Path parent = new Path("/d1/d2/d3/d4/");
    Path file1 = new Path(parent, "key1");
    try (FSDataOutputStream outputStream = fs.create(file1, false)) {
      assertNotNull("Should be able to create file", outputStream);
    }

    Path dir1 = new Path("/d1/d2/d3/d4/key2");
    fs.mkdirs(dir1);
    try (FSDataOutputStream outputStream1 = fs.create(dir1, false)) {
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae){
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
    try (FSDataOutputStream outputStream2 = fs.create(file3, false)) {
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae) {
      // ignore as its expected
    }
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has roughly the semantics of Unix @{code mkdir -p}.
   * {@link FileSystem#mkdirs(Path)}
   */
  @Test(timeout = 300_000)
  public void testMakeDirsWithAnExistingDirectoryPath() throws Exception {
    /*
     * Op 1. create file -> /d1/d2/d3/d4/k1 (d3 is a sub-dir inside /d1/d2)
     * Op 2. create dir -> /d1/d2
     */
    setupOzoneFileSystem();

    Path parent = new Path("/d1/d2/d3/d4/");
    Path file1 = new Path(parent, "key1");
    try (FSDataOutputStream outputStream = fs.create(file1, false)) {
      assertNotNull("Should be able to create file", outputStream);
    }

    Path subdir = new Path("/d1/d2/");
    boolean status = fs.mkdirs(subdir);
    assertTrue("Shouldn't send error if dir exists", status);
  }

  @Test(timeout = 300_000)
  public void testFileSystem() throws Exception {
    setupOzoneFileSystem();

    testOzoneFsServiceLoader();
    o3fs = (OzoneFileSystem) fs;

    testGetDirectoryModificationTime();

    testListStatusOnRoot();
    testListStatus();
    testListStatusOnSubDirs();
    testListStatusOnLargeDirectory();

    testCreateDoesNotAddParentDirKeys();
    testDeleteCreatesFakeParentDir();
    testFileDelete();
    testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved();

    testRenameDir();
    testSeekOnFileLength();
    testDeleteRoot();
  }

  @After
  public void tearDown() {
    IOUtils.closeQuietly(fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void setupOzoneFileSystem()
          throws IOException, TimeoutException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
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

  private void testOzoneFsServiceLoader() throws IOException {
    assertEquals(
        FileSystem.getFileSystemClass(OzoneConsts.OZONE_URI_SCHEME, null),
        OzoneFileSystem.class);
  }

  private void testCreateDoesNotAddParentDirKeys() throws Exception {
    Path grandparent = new Path("/testCreateDoesNotAddParentDirKeys");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);
    rootItemCount++; // grandparent

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

  private void testDeleteCreatesFakeParentDir() throws Exception {
    Path grandparent = new Path("/testDeleteCreatesFakeParentDir");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    ContractTestUtils.touch(fs, child);
    rootItemCount++; // grandparent

    // Verify that parent dir key does not exist
    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (IOException ex) {
      assertKeyNotFoundException(ex);
    }

    // Delete the child key
    fs.delete(child, false);

    // Deleting the only child should create the parent dir key if it does
    // not exist
    String parentKey = o3fs.pathToKey(parent) + "/";
    OzoneKeyDetails parentKeyInfo = getKey(parent, true);
    assertEquals(parentKey, parentKeyInfo.getName());
  }

  private void testFileDelete() throws Exception {
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

    assertTrue(fs.listStatus(grandparent).length == 1);
    assertTrue(fs.listStatus(parent).length == 9);
    assertTrue(fs.listStatus(childFolder).length == 8);

    Boolean successResult = fs.delete(grandparent, true);
    assertTrue(successResult);
    assertTrue(!o3fs.exists(grandparent));
    for (int i = 0; i < 8; i++) {
      Path childFile = new Path(parent, "child" + i);
      // Make sure all keys under testBatchDelete/parent should be deleted
      assertTrue(!o3fs.exists(childFile));

      // Test to recursively delete child folder, make sure all keys under
      // testBatchDelete/parent/childFolder should be deleted.
      Path childFolderFile = new Path(childFolder, "child" + i);
      assertTrue(!o3fs.exists(childFolderFile));
    }
    // Will get: WARN  ozone.BasicOzoneFileSystem delete: Path does not exist.
    // This will return false.
    Boolean falseResult = fs.delete(parent, true);
    assertFalse(falseResult);

  }

  private void testListStatus() throws Exception {
    Path parent = new Path("/testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");
    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);
    rootItemCount++; // parent

    // ListStatus on a directory should return all subdirs along with
    // files, even if there exists a file and sub-dir with the same name.
    FileStatus[] fileStatuses = o3fs.listStatus(parent);
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
    setupOzoneFileSystem();
    String keyName = "object-dir/object-name";
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setLocationInfoList(new ArrayList<>())
        .build();

    OpenKeySession session = cluster.getOzoneManager().openKey(keyArgs);
    cluster.getOzoneManager().commitKey(keyArgs, session.getId());

    Path parent = new Path("/");
    FileStatus[] fileStatuses = fs.listStatus(parent);

    // the number of immediate children of root is 1
    Assert.assertEquals(1, fileStatuses.length);
    cluster.getOzoneManager().deleteKey(keyArgs);
  }

  /**
   * Tests listStatus operation on root directory.
   */
  private void testListStatusOnRoot() throws Exception {
    Path root = new Path("/");
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    fs.mkdirs(dir12);
    rootItemCount++; // dir1
    fs.mkdirs(dir2);
    rootItemCount++; // dir2

    // ListStatus on root should return dir1 (even though /dir1 key does not
    // exist) and dir2 only. dir12 is not an immediate child of root and
    // hence should not be listed.
    FileStatus[] fileStatuses = o3fs.listStatus(root);
    assertEquals("FileStatus should return only the immediate children",
        rootItemCount, fileStatuses.length);

    // Verify that dir12 is not included in the result of the listStatus on root
    String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
    String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
    assertNotEquals(fileStatus1, dir12.toString());
    assertNotEquals(fileStatus2, dir12.toString());
  }

  /**
   * Tests listStatus operation on root directory.
   */
  private void testListStatusOnLargeDirectory() throws Exception {
    Path root = new Path("/");
    Set<String> paths = new TreeSet<>();
    int numDirs = 5111;
    for(int i = 0; i < numDirs; i++) {
      Path p = new Path(root, String.valueOf(i));
      fs.mkdirs(p);
      paths.add(p.getName());
      rootItemCount++;
    }

    FileStatus[] fileStatuses = o3fs.listStatus(root);
    assertEquals(
        "Total directories listed do not match the existing directories",
        rootItemCount, fileStatuses.length);

    for (int i=0; i < numDirs; i++) {
      assertTrue(paths.contains(fileStatuses[i].getPath().getName()));
    }
  }

  /**
   * Tests listStatus on a path with subdirs.
   */
  private void testListStatusOnSubDirs() throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatus on /dir1 should return all its immediated subdirs only
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

  public void testSeekOnFileLength() throws IOException {
    Path file = new Path("/file");
    ContractTestUtils.createFile(fs, file, true, "a".getBytes());
    try (FSDataInputStream stream = fs.open(file)) {
      long fileLength = fs.getFileStatus(file).getLen();
      stream.seek(fileLength);
      assertEquals(-1, stream.read());
    }
  }

  public void testDeleteRoot() throws IOException {
    Path dir = new Path("/dir");
    fs.mkdirs(dir);
    assertFalse(fs.delete(new Path("/"), true));
    assertNotNull(fs.getFileStatus(dir));
  }

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

  private void testRenameDir() throws Exception {
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
    LambdaTestUtils.intercept(IllegalArgumentException.class, "Wrong FS",
        () -> fs.rename(new Path(fs.getUri().toString() + "fake" + dir), dest));

    // Renaming to same path when src is specified with scheme.
    assertTrue("Renaming to same path should be success.",
        fs.rename(source, new Path(dir)));
  }
  private OzoneKeyDetails getKey(Path keyPath, boolean isDirectory)
      throws IOException {
    String key = o3fs.pathToKey(keyPath);
    if (isDirectory) {
      key = key + "/";
    }
    return cluster.getClient().getObjectStore().getVolume(volumeName)
        .getBucket(bucketName).getKey(key);
  }

  private void assertKeyNotFoundException(IOException ex) {
    GenericTestUtils.assertExceptionContains("KEY_NOT_FOUND", ex);
  }

  private void testGetDirectoryModificationTime()
      throws IOException, InterruptedException {
    Path mdir1 = new Path("/mdir1");
    Path mdir11 = new Path(mdir1, "mdir11");
    Path mdir111 = new Path(mdir11, "mdir111");
    fs.mkdirs(mdir111);
    rootItemCount++; // mdir1

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
}
