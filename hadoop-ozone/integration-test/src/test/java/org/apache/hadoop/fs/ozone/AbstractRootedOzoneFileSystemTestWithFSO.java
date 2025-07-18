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

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests to verify ofs with prefix enabled cases.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractRootedOzoneFileSystemTestWithFSO extends AbstractRootedOzoneFileSystemTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRootedOzoneFileSystemTestWithFSO.class);

  AbstractRootedOzoneFileSystemTestWithFSO(boolean isAclEnabled) {
    super(BucketLayout.FILE_SYSTEM_OPTIMIZED, true, isAclEnabled);
  }

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Override
  @Test
  void testRenameDestinationParentDoesNotExist() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(getBucketPath() + dir2);
    getFs().mkdirs(dir2SourcePath);
    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(getBucketPath()
        + root + "/b/c");

    // rename should fail and return false
    assertFalse(getFs().rename(dir2SourcePath, destinPath));
    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(getBucketPath() + root + "/file1");
    ContractTestUtils.touch(getFs(), filePath);
    Path newDestinPath = new Path(filePath, "c");
    // rename should fail and return false
    assertFalse(getFs().rename(dir2SourcePath, newDestinPath));
  }

  @Test
  void testKeyRenameToBucketLevel() throws IOException {
    final String dir = "dir1";
    final String key = dir + "/key1";
    final Path source = new Path(getBucketPath(), key);
    getFs().mkdirs(source);
    final Path dest = new Path(String.valueOf(getBucketPath()));
    LOG.info("Will move {} to {}", source, dest);
    getFs().rename(source, getBucketPath());
    assertTrue(getFs().exists(new Path(getBucketPath(), "key1")),
        "Key rename failed");
    // cleanup
    getFs().delete(dest, true);
  }

  @Test
  void testRenameDir() throws Exception {
    final String dir = "dir1";
    final Path source = new Path(getBucketPath(), dir);
    final Path dest = new Path(source + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    getFs().mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    getFs().rename(source, dest);
    assertTrue(getFs().exists(dest), "Directory rename failed");
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // subdirectories of the renamed directory have also been renamed.
    assertTrue(getFs().exists(new Path(dest, "sub_dir1")),
        "Keys under the renamed directory not renamed");
    // cleanup
    getFs().delete(dest, true);
  }

  /**
   *  Cannot rename a directory to its own subdirectory.
   */
  @Override
  @Test
  void testRenameDirToItsOwnSubDir() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(getBucketPath() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    final Path sourceRoot = new Path(getBucketPath() + root);
    try {
      getFs().mkdirs(subDir1);
      LOG.info("Created dir1 {}", subDir1);
      LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
      //  rename should fail and return false
      assertFalse(getFs().rename(sourceRoot, subDir1));
    } finally {
      getFs().delete(sourceRoot, true);
    }
  }

  @Override
  @Test
  void testDeleteVolumeAndBucket() throws IOException {
    String volumeStr1 = getRandomNonExistVolumeName();
    Path volumePath1 = new Path(OZONE_URI_DELIMITER + volumeStr1);
    String bucketStr2 = "bucket3";
    Path bucketPath2 = new Path(volumePath1, bucketStr2);

    for (int i = 1; i <= 5; i++) {
      String dirStr1 = "dir1" + i;
      String dirStr2 = "dir2" + i;
      String fileStr3 = "file3" + i;
      Path dirLevel1 = new Path(bucketPath2, dirStr1);
      Path dirLevel2 = new Path(dirLevel1, dirStr2);
      getFs().mkdirs(dirLevel2);
      Path filePath3 = new Path(dirLevel2, fileStr3);

      try (FSDataOutputStream out1 = getFs().create(filePath3)) {
        out1.write(2);
      }
    }
    // create another top level file
    Path file16 = new Path(bucketPath2, "file16");
    try (FSDataOutputStream out1 = getFs().create(file16)) {
      out1.write(2);
    }

    /*         bucket
        _________|_________________________
        |       |      |     |      |     |
       dir11   dir12  dir13 dir14 dir15   file16
        |       |      |      |     |
       dir21   dir22  dir23 dir24  dir25
        |       |      |      |     |
        file31  file32 file33 file34 file35

        Total dirs =10 , files = 6 , keys = 16

     */

    long prevDeletes = getOMMetrics().getNumKeyDeletes();
    assertTrue(getFs().delete(bucketPath2, true));
    assertTrue(getFs().delete(volumePath1, false));
    long deletes = getOMMetrics().getNumKeyDeletes();
    assertEquals(prevDeletes + 1, deletes);
  }

  /**
   * Test the consistency of listStatusFSO with TableCache present.
   */
  @Test
  void testListStatusFSO() throws Exception {
    // list keys batch size is 1024. Creating keys greater than the
    // batch size to test batch listing of the keys.
    int valueGreaterBatchSize = 1200;
    Path parent = new Path(getBucketPath(), "testListStatusFSO");
    for (int i = 0; i < valueGreaterBatchSize; i++) {
      Path key = new Path(parent, "tempKey" + i);
      ContractTestUtils.touch(getFs(), key);
      /*
      To add keys to the cache. listStatusFSO goes through the cache first.
      The cache is not continuous and may be greater than the batch size.
      This may cause inconsistency in the listing of keys.
       */
      getFs().rename(key, new Path(parent, "key" + i));
    }

    FileStatus[] fileStatuses = getFs().listStatus(
        new Path(getBucketPath() + "/testListStatusFSO"));
    assertEquals(valueGreaterBatchSize, fileStatuses.length);
  }

  @Test
  void testLeaseRecoverable() throws Exception {
    // Create a file
    final String dir = "dir1";
    final String key = dir + "/key1";
    final Path source = new Path(getBucketPath(), key);

    LeaseRecoverable fs = (LeaseRecoverable)getFs();
    FSDataOutputStream stream = getFs().create(source);
    try {
      assertThrows(FileNotFoundException.class, () -> fs.isFileClosed(source));
      stream.write(1);
      stream.hsync();
      assertFalse(fs.isFileClosed(source));
      assertTrue(fs.recoverLease(source));
      assertTrue(fs.isFileClosed(source));
    } finally {
      TestLeaseRecovery.closeIgnoringKeyNotFound(stream);
    }
  }
}
