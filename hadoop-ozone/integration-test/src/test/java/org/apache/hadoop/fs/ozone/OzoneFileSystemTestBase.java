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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;

/**
 * Common test cases for Ozone file systems.
 */
public abstract class OzoneFileSystemTestBase {

  protected OzoneFileSystemTestBase() {
    // no instances
  }

  /**
   * Tests listStatusIterator operation on directory with different
   * numbers of child directories.
   */
  protected void listStatusIteratorOnPageSize(OzoneConfiguration conf,
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

  private void listStatusIterator(FileSystem subject,
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

  protected void listLocatedStatusForZeroByteFile(FileSystem fs, Path path) throws IOException {
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

  protected void createKeyWithECReplicationConfig(Path root, OzoneConfiguration conf) throws IOException {
    Path testKeyPath = new Path(root, "testKey");
    createKeyWithECReplicationConfiguration(conf, testKeyPath);

    OzoneKeyDetails key = getKey(testKeyPath, false);
    assertEquals(HddsProtos.ReplicationType.EC,
        key.getReplicationConfig().getReplicationType());
    assertEquals("rs-3-2-1024k",
        key.getReplicationConfig().getReplication());
  }

  protected void listStatusIteratorWithDir(Path root, FileSystem fs) throws Exception {
    Path parent = new Path(root, "testListStatus");
    Path file1 = new Path(parent, "key1");
    Path file2 = new Path(parent, "key2");
    try {
      // Iterator should have no items when dir is empty
      RemoteIterator<FileStatus> it = listStatusIterator(root);
      assertFalse(it.hasNext());

      ContractTestUtils.touch(fs, file1);
      ContractTestUtils.touch(fs, file2);
      // Iterator should have an item when dir is not empty
      it = listStatusIterator(root);
      while (it.hasNext()) {
        FileStatus fileStatus = it.next();
        assertNotNull(fileStatus);
        assertEquals(fileStatus.getPath().toUri().getPath(), parent.toString(), "Parent path doesn't match");
      }
      // Iterator on a directory should return all subdirs along with
      // files, even if there exists a file and sub-dir with the same name.
      it = listStatusIterator(parent);
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
      it = listStatusIterator(parent);
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

  abstract RemoteIterator<FileStatus> listStatusIterator(Path path) throws IOException;

  abstract OzoneKeyDetails getKey(Path keyPath, boolean isDirectory) throws IOException;
}
