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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.commons.io.IOUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone file system tests that are not covered by contract tests.
 */
public class TestOzoneFileSystem {

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
    try (FSDataOutputStream outputStream1 = fs.create(dir1, true)) {
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

    // overwrite = false
    try (FSDataOutputStream outputStream2 = fs.create(file2, false)) {
      fail("Should throw FileAlreadyExistsException");
    } catch (FileAlreadyExistsException fae) {
      // ignore as its expected
    }
    // overwrite = true
    try (FSDataOutputStream outputStream = fs.create(file2, true)) {
      assertNotNull("Should be able to create file", outputStream);
    }

    // Op 3. create file -> /d1/d2/d3 (d3 as a file inside /d1/d2)
    Path file3 = new Path("/d1/d2/d3");
    try (FSDataOutputStream outputStream2 = fs.create(file3, false)) {
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

    // invalid
    try{
      fs.getFileStatus(new Path("/d1/d2/d3/d4/key3" +
              "/invalid"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
  }

  @Test
  public void testListStatusWithoutRecursiveSearch() throws Exception {
    /*
     * Op 1. create file -> /key1
     * Op 2. create dir -> /d1/d2
     * Op 3. create dir -> /d1/d3
     * Op 4. create dir -> /d1/d4
     * Op 5. create file -> /d1/key1
     * Op 6. create file -> /d2/key1
     * Op 7. create file -> /d1/d2/key1
     */
    setupOzoneFileSystem();

    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = fs.create(key1, false)) {
      assertNotNull("Should be able to create file: key1",
              outputStream);
    }
    Path d1 = new Path("/d1");
    Path d1_key1 = new Path(d1, "key1");
    try (FSDataOutputStream outputStream = fs.create(d1_key1, false)) {
      assertNotNull("Should be able to create file: " + d1_key1,
              outputStream);
    }
    Path d2 = new Path("/d2");
    Path d2_key1 = new Path(d2, "key1");
    try (FSDataOutputStream outputStream = fs.create(d2_key1, false)) {
      assertNotNull("Should be able to create file: " + d2_key1,
              outputStream);
    }
    Path d1_d2 = new Path("/d1/d2/");
    Path d1_d2_key1 = new Path(d1_d2, "key1");
    try (FSDataOutputStream outputStream = fs.create(d1_d2_key1, false)) {
      assertNotNull("Should be able to create file: " + d1_d2_key1,
              outputStream);
    }
    Path d1_key2 = new Path(d1, "key2");
    try (FSDataOutputStream outputStream = fs.create(d1_key2, false)) {
      assertNotNull("Should be able to create file: " + d1_key2,
              outputStream);
    }

    Path d1_d3 = new Path("/d1/d3/");
    Path d1_d4 = new Path("/d1/d4/");

    fs.mkdirs(d1_d3);
    fs.mkdirs(d1_d4);

    // Root Directory
    FileStatus[] fileStatusList = fs.listStatus(new Path("/"));
    assertEquals("FileStatus should return files and directories",
            3, fileStatusList.length);
    ArrayList<String> expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-1 sub-dirs
    fileStatusList = fs.listStatus(new Path("/d1"));
    assertEquals("FileStatus should return files and directories",
            5, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d3");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d4");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key1");
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/key2");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-2 sub-dirs
    fileStatusList = fs.listStatus(new Path("/d1/d2"));
    assertEquals("FileStatus should return files and directories",
            1, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // level-2 key2
    fileStatusList = fs.listStatus(new Path("/d1/d2/key1"));
    assertEquals("FileStatus should return files and directories",
            1, fileStatusList.length);
    expectedPaths = new ArrayList<>();
    expectedPaths.add("o3fs://" + bucketName + "." + volumeName + "/d1/d2/" +
            "key1");
    for (FileStatus fileStatus : fileStatusList) {
      expectedPaths.remove(fileStatus.getPath().toString());
    }
    assertEquals("Failed to return the filestatus[]" + expectedPaths,
            0, expectedPaths.size());

    // invalid root key
    try {
      fileStatusList = fs.listStatus(new Path("/key2"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
    try {
      fileStatusList = fs.listStatus(new Path("/d1/d2/key2"));
      fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // ignore as its expected
    }
  }


  @Test
  public void testListFilesRecursive() throws Exception {
    /*
     * Op 1. create file -> /d1/d1/d2/key1
     * Op 2. create dir -> /key1
     * Op 3. create dir -> /key2
     * Op 4. create dir -> /d1/d2/d1/d2/key1
     */
    setupOzoneFileSystem();

    Path d1_d1_d2_key1 = new Path("/d1/d1/d2/key1");
    try (FSDataOutputStream outputStream = fs.create(d1_d1_d2_key1, false)) {
      assertNotNull("Should be able to create file: " + d1_d1_d2_key1,
              outputStream);
    }
    Path key1 = new Path("/key1");
    try (FSDataOutputStream outputStream = fs.create(key1, false)) {
      assertNotNull("Should be able to create file: " + key1,
              outputStream);
    }
    Path key2 = new Path("/key2");
    try (FSDataOutputStream outputStream = fs.create(key2, false)) {
      assertNotNull("Should be able to create file: key2",
              outputStream);
    }
    Path d1_d2_d1_d2_key1 = new Path("/d1/d2/d1/d2/key1");
    try (FSDataOutputStream outputStream = fs.create(d1_d2_d1_d2_key1, false)) {
      assertNotNull("Should be able to create file: " + d1_d2_d1_d2_key1,
              outputStream);
    }
    RemoteIterator<LocatedFileStatus> fileStatusItr = fs.listFiles(new Path(
            "/"), true);
    String uriPrefix = "o3fs://" + bucketName + "." + volumeName;
    ArrayList<String> expectedPaths = new ArrayList<>();
    expectedPaths.add(uriPrefix + d1_d1_d2_key1.toString());
    expectedPaths.add(uriPrefix + key1.toString());
    expectedPaths.add(uriPrefix + key2.toString());
    expectedPaths.add(uriPrefix + d1_d2_d1_d2_key1.toString());
    int expectedFilesCount = expectedPaths.size();
    int actualCount = 0;
    while (fileStatusItr.hasNext()) {
      LocatedFileStatus status = fileStatusItr.next();
      expectedPaths.remove(status.getPath().toString());
      actualCount++;
    }
    assertEquals("Failed to get all the files: " + expectedPaths,
            expectedFilesCount, actualCount);
    assertEquals("Failed to get all the files: " + expectedPaths, 0,
            expectedPaths.size());

    // Recursive=false
    fileStatusItr = fs.listFiles(new Path("/"), false);
    expectedPaths.clear();
    expectedPaths.add(uriPrefix + "/key1");
    expectedPaths.add(uriPrefix + "/key2");
    expectedFilesCount = expectedPaths.size();
    actualCount = 0;
    while (fileStatusItr.hasNext()) {
      LocatedFileStatus status = fileStatusItr.next();
      expectedPaths.remove(status.getPath().toString());
      actualCount++;
    }
    assertEquals("Failed to get all the files: " + expectedPaths, 0,
            expectedPaths.size());
    assertEquals("Failed to get all the files: " + expectedPaths,
            expectedFilesCount, actualCount);
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
    testNonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved();

    // TODO: [HDDS-2939] rename should be supported
    testRenameDir();
    testSeekOnFileLength();
    testDeleteDir();
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

    // Child key creation should create all the missing parents.
    String parentKey = o3fs.pathToKey(parent) + "/";
    OzoneKeyDetails parentKeyInfo = getKey(parent, true);
    assertEquals(parentKey, parentKeyInfo.getName());

    // Delete the child key
    fs.delete(child, false);

    // Deleting the only child should not delete the parent key.
    parentKeyInfo = getKey(parent, true);
    assertEquals(parentKey, parentKeyInfo.getName());
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

    // Add second level sub dirs and do list files on sub dir parent.
    int subDir = numDirs / 2;
    Path subDirPath = new Path(root + String.valueOf(subDir),
            String.valueOf(subDir));
    fs.mkdirs(subDirPath);
    String subDirName = subDirPath.getName();
    Path subDirParent = subDirPath.getParent();
    FileStatus[] fileStatuses = o3fs.listStatus(subDirParent);
    assertEquals("Failed to list dirs", 1,
            fileStatuses.length);
    assertEquals("Failed to list dirs",
            subDirName, fileStatuses[0].getPath().getName());

    // List Files of root directory should only list immediate child.
    fileStatuses = o3fs.listStatus(root);
    assertEquals(
        "Total directories listed do not match the existing directories",
        rootItemCount, fileStatuses.length);

    for (int i = 0; i < numDirs; i++) {
      assertTrue(paths.contains(fileStatuses[i].getPath().getName()));
      String subDirPathname = "o3fs://" + bucketName + "." + volumeName
              + subDirPath.toString();
      assertFalse("Sub directories: " + subDirName + " other than " +
                      "immediate child shouldn't be listed as " +
                      "recursive flag is false from ozoneclient-to-server. " +
                      "File name: " + fileStatuses[i].getPath().getName(),
              subDirPathname.equals(fileStatuses[i].getPath().toString()));
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

  public void testDeleteDir() throws IOException {
    Path dir1 = new Path("/dir/sub-dir1");
    fs.mkdirs(dir1);
    Path dir2 = new Path("/dir/sub-dir2");
    fs.mkdirs(dir2);
    assertTrue("Failed to delete dir!", fs.delete(new Path("/dir"), true));
    try{
      fs.getFileStatus(new Path("/dir"));
      fail("Should throw IOE as /dir doesn't exists!");
    } catch(IOException ioe) {
      // expected
    }
    try{
      fs.getFileStatus(new Path("/dir/sub-dir1"));
      fail("Should throw IOE as /dir/sub-dir1 doesn't exists!");
    } catch(IOException ioe) {
      // expected
    }
    try{
      fs.getFileStatus(new Path("/dir/sub-dir2"));
      fail("Should throw IOE as /dir/sub-dir2 doesn't exists!");
    } catch(IOException ioe) {
      // expected
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
    // TODO: [HDDS-2939]rename should be supported
    /* FileStatus[] statuses = fs.listStatus(interimPath);
    assertNotNull("liststatus returns a null array", statuses);
    assertEquals("Statuses array is not empty", 0, statuses.length);
    FileStatus fileStatus = fs.getFileStatus(interimPath);
    assertEquals("FileStatus does not point to interimPath",
        interimPath.getName(), fileStatus.getPath().getName());
     */
  }

  private void testRenameDir() throws Exception {
    final String dir = "/root_dir/dir1";

    // Test-1: RENAME when destination path doesn't exists
    final Path source = new Path(fs.getUri().toString() + dir);
    final Path dest = new Path(source.toString() + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    fs.mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    assertTrue("Directory rename failed", fs.rename(source, dest));
    // destin should exists
    assertTrue("Directory rename failed", fs.exists(dest));
    // source should be deleted
    assertFalse("Directory rename failed", fs.exists(source));
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue("Keys under the renamed directory not renamed",
        fs.exists(new Path(dest, "sub_dir1")));

    // Test-2: RENAME when destination path exists
    final Path newdest = new Path("/root_dir/newdir");
    fs.mkdirs(newdest);
    assertTrue("Destin directory doesn't exists", fs.exists(newdest));
    final Path newsrc = dest;
    assertTrue("Source directory doesn't exists", fs.exists(newsrc));
    assertTrue("Directory rename failed", fs.rename(newsrc, newdest));
    Path expectedDest = new Path(newdest, newsrc.getName());
    assertTrue("Directory rename failed", fs.exists(expectedDest));
    // source should be deleted
    assertFalse("Directory rename failed", fs.exists(newsrc));

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
