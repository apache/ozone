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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION_TYPE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Common test cases for Ozone file systems.
 */
public abstract class OzoneFileSystemTestBase {
  /**
   * Tests listStatusIterator operation on directory with different
   * numbers of child directories.
   */
  static void listStatusIteratorOnPageSize(OzoneConfiguration conf,
      String rootPath) throws IOException {
    final int pageSize = 32;
    int[] dirCounts = {
        1,
        pageSize - 1,
        pageSize,
        pageSize + 1,
        pageSize + pageSize / 2,
        pageSize + pageSize
    };
    OzoneConfiguration config = new OzoneConfiguration(conf);
    OzoneFileSystemTestUtils.setPageSize(config, pageSize);
    URI uri = FileSystem.getDefaultUri(config);
    try (FileSystem subject = FileSystem.get(uri, config)) {
      Path dir = new Path(Objects.requireNonNull(rootPath),
          "listStatusIterator");
      try {
        Set<String> paths = new TreeSet<>();
        for (int dirCount : dirCounts) {
          listStatusIterator(subject, dir, paths, dirCount);
        }
      } finally {
        subject.delete(dir, true);
      }
    }
  }

  private static void listStatusIterator(FileSystem subject,
      Path dir, Set<String> paths, int total) throws IOException {
    for (int i = paths.size(); i < total; i++) {
      Path p = new Path(dir, String.valueOf(i));
      subject.mkdirs(p);
      paths.add(p.getName());
    }

    RemoteIterator<FileStatus> iterator = subject.listStatusIterator(dir);
    int iCount = 0;
    if (iterator != null) {
      while (iterator.hasNext()) {
        FileStatus fileStatus = iterator.next();
        iCount++;
        String filename = fileStatus.getPath().getName();
        assertThat(paths).contains(filename);
      }
    }

    assertEquals(total, iCount);
  }

  private void createKeyWithECReplicationConfiguration(OzoneConfiguration inputConf, Path keyPath)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration(inputConf);
    conf.set(OZONE_REPLICATION, "rs-3-2-1024k");
    conf.set(OZONE_REPLICATION_TYPE, "EC");
    URI uri = FileSystem.getDefaultUri(conf);
    conf.setBoolean(
        String.format("fs.%s.impl.disable.cache", uri.getScheme()), true);
    try (FileSystem fileSystem = FileSystem.get(uri, conf)) {
      ContractTestUtils.touch(fileSystem, keyPath);
    }
  }

  static void listLocatedStatusForZeroByteFile(FileSystem fs, Path path) throws IOException {
    // create empty file
    ContractTestUtils.touch(fs, path);

    RemoteIterator<LocatedFileStatus> listLocatedStatus = fs.listLocatedStatus(path);
    int count = 0;

    while (listLocatedStatus.hasNext()) {
      LocatedFileStatus locatedFileStatus = listLocatedStatus.next();
      assertEquals(0, locatedFileStatus.getLen());
      BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
      assertNotNull(blockLocations);
      assertEquals(0, blockLocations.length);

      count++;
    }
    assertEquals(1, count);

    count = 0;
    RemoteIterator<FileStatus> listStatus = fs.listStatusIterator(path);
    while (listStatus.hasNext()) {
      FileStatus fileStatus = listStatus.next();
      assertEquals(0, fileStatus.getLen());
      assertFalse(fileStatus instanceof LocatedFileStatus);
      count++;
    }
    assertEquals(1, count);

    FileStatus[] fileStatuses = fs.listStatus(path.getParent());
    assertEquals(1, fileStatuses.length);
    assertFalse(fileStatuses[0] instanceof LocatedFileStatus);
  }

  void createKeyWithECReplicationConfig(Path root, OzoneConfiguration conf) throws IOException {
    Path testKeyPath = new Path(root, "testKey");
    createKeyWithECReplicationConfiguration(conf, testKeyPath);

    OzoneKeyDetails key = getKey(testKeyPath, false);
    assertEquals(HddsProtos.ReplicationType.EC,
        key.getReplicationConfig().getReplicationType());
    assertEquals("rs-3-2-1024k",
        key.getReplicationConfig().getReplication());
  }

  void listStatusIteratorWithDir(Path root) throws Exception {
    Path parent = new Path(root, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");
    FileSystem fs = getFs();
    try {
      // Iterator should have no items when dir is empty
      RemoteIterator<FileStatus> it = fs.listStatusIterator(root);
      assertFalse(it.hasNext());

      ContractTestUtils.touch(fs, file1);
      ContractTestUtils.touch(fs, file2);
      // Iterator should have an item when dir is not empty
      it = fs.listStatusIterator(root);
      while (it.hasNext()) {
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        assertEquals(fileStatus.getPath().toUri().getPath(), parent.toString(), "Parent path doesn't match");
      }
      // Iterator on a directory should return all subdirs along with
      // files, even if there exists a file and sub-dir with the same name.
      it = fs.listStatusIterator(parent);
      int iCount = 0;
      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
      }
      assertEquals(2, iCount, "Iterator did not return all the file status");
      // Iterator should return file status for only the
      // immediate children of a directory.
      Path file3 = new Path(parent, "dir1/key3");
      Path file4 = new Path(parent, "dir1/key4");
      ContractTestUtils.touch(fs, file3);
      ContractTestUtils.touch(fs, file4);
      it = fs.listStatusIterator(parent);
      iCount = 0;

      while (it.hasNext()) {
        iCount++;
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
      }
      assertEquals(3, iCount, "Iterator did not return file status " +
          "of all the children of the directory");

    } finally {
      // Cleanup
      fs.delete(parent, true);
    }
  }

  void createDoesNotAddParentDirKeys(Path grandparent) throws Exception {
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    FileSystem fs = getFs();
    ContractTestUtils.touch(fs, child);

    OzoneKeyDetails key = getKey(child, false);
    String expectedKeyName = getChildKeyName(child);
    assertEquals(key.getName(), expectedKeyName);

    // Creating a child should not add parent keys to the bucket
    try {
      getKey(parent, true);
    } catch (OMException ome) {
      assertEquals(KEY_NOT_FOUND, ome.getResult());
    }

    // List status on the parent should show the child file
    assertEquals(1L, fs.listStatus(parent).length, "List status of parent should include the 1 child file");
    assertTrue(fs.getFileStatus(parent).isDirectory(), "Parent directory does not appear to be a directory");

    // Cleanup
    fs.delete(grandparent, true);
  }

  abstract String getChildKeyName(Path child);

  void listStatusIteratorOnRoot(Path root) throws Exception {
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    FileSystem fs = getFs();
    try {
      fs.mkdirs(dir12);
      fs.mkdirs(dir2);

      // ListStatusIterator on root should return dir1
      // (even though /dir1 key does not exist)and dir2 only.
      // dir12 is not an immediate child of root and hence should not be listed.
      RemoteIterator<FileStatus> it = fs.listStatusIterator(root);
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

  void deleteCreatesFakeParentDir(Path root) throws IOException {
    Path grandparent = new Path(root, "testDeleteCreatesFakeParentDir");
    Path parent = new Path(grandparent, "parent");
    Path child = new Path(parent, "child");
    FileSystem fs = getFs();
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
    verifyDeleteCreatesFakeParentDir(parent);

    // Recursive delete with DeleteIterator
    assertTrue(fs.delete(grandparent, true));
  }

  void verifyListStatus(Path root, PathFilter filter) throws Exception {
    Path parent = new Path(root, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");
    FileSystem fs = getFs();

    FileStatus[] fileStatuses = filter != null
        ? fs.listStatus(root, filter)
        : fs.listStatus(root);
    assertEquals(0, fileStatuses.length, "Should be empty");

    ContractTestUtils.touch(fs, file1);
    ContractTestUtils.touch(fs, file2);

    fileStatuses = filter != null
        ? fs.listStatus(root, filter)
        : fs.listStatus(root);
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

    // Cleanup
    fs.delete(parent, true);
  }

  void listStatusIteratorOnSubDirs(Path root) throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // listStatusIterator on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatusIterator.
    Path dir1 = new Path(root, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(root, "dir2");
    FileSystem fs = getFs();
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
        // Verify that the two children of /dir1
        // returned by listStatusIterator operation
        // are /dir1/dir11 and /dir1/dir12.
        assertTrue(fileStatus.getPath().toUri().getPath().
            equals(dir11.toString()) ||
            fileStatus.getPath().toUri().getPath().
                equals(dir12.toString()));
      }
      assertEquals(2, iCount, "Iterator should return only the immediate children");
    } finally {
      // Cleanup
      fs.delete(dir2, true);
      fs.delete(dir1, true);
    }
  }

  void listStatusOnRoot(Path root, PathFilter filter) throws Exception {
    Path dir1 = new Path(root, "dir1");
    Path dir12 = new Path(dir1, "dir12");
    Path dir2 = new Path(root, "dir2");
    FileSystem fs = getFs();
    try {
      fs.mkdirs(dir12);
      fs.mkdirs(dir2);

      // ListStatus on root should return dir1 (even though /dir1 key does not
      // exist) and dir2 only. dir12 is not an immediate child of root and
      // hence should not be listed.
      FileStatus[] fileStatuses = filter != null
          ? fs.listStatus(root, filter)
          : fs.listStatus(root);
      assertEquals(2, fileStatuses.length, "FileStatus should return only the immediate children");

      // Verify that dir12 is not included in the result of the listStatus on
      // root
      String fileStatus1 = fileStatuses[0].getPath().toUri().getPath();
      String fileStatus2 = fileStatuses[1].getPath().toUri().getPath();
      assertNotEquals(fileStatus1, dir12.toString());
      assertNotEquals(fileStatus2, dir12.toString());
    } finally {
      // cleanup
      fs.delete(dir1, true);
      fs.delete(dir2, true);
    }
  }

  void listStatusOnSubDirs(Path root) throws Exception {
    // Create the following key structure
    //      /dir1/dir11/dir111
    //      /dir1/dir12
    //      /dir1/dir12/file121
    //      /dir2
    // ListStatus on /dir1 should return all its immediated subdirs only
    // which are /dir1/dir11 and /dir1/dir12. Super child files/dirs
    // (/dir1/dir12/file121 and /dir1/dir11/dir111) should not be returned by
    // listStatus.
    Path dir1 = new Path(root, "dir1");
    Path dir11 = new Path(dir1, "dir11");
    Path dir111 = new Path(dir11, "dir111");
    Path dir12 = new Path(dir1, "dir12");
    Path file121 = new Path(dir12, "file121");
    Path dir2 = new Path(root, "dir2");
    FileSystem fs = getFs();
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

    // Cleanup
    fs.delete(dir2, true);
    fs.delete(dir1, true);
  }

  void nonExplicitlyCreatedPathExistsAfterItsLeafsWereRemoved(Path root)
      throws Exception {
    Path source = new Path(root, "source");
    Path interimPath = new Path(source, "interimPath");
    Path leafInsideInterimPath = new Path(interimPath, "leaf");
    Path target = new Path(root, "target");
    Path leafInTarget = new Path(target, "leaf");
    FileSystem fs = getFs();

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

    // Cleanup
    fs.delete(target, true);
    fs.delete(source, true);
  }

  abstract void verifyDeleteCreatesFakeParentDir(Path parent) throws IOException;

  abstract OzoneKeyDetails getKey(Path keyPath, boolean isDirectory) throws IOException;

  abstract FileSystem getFs();
}
