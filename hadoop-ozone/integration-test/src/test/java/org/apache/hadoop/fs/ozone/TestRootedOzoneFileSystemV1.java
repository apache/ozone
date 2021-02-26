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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestRootedOzoneFileSystemV1 extends TestRootedOzoneFileSystem {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRootedOzoneFileSystemV1.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  public TestRootedOzoneFileSystemV1(boolean setDefaultFs,
      boolean enableOMRatis) throws Exception {
    super(setDefaultFs, enableOMRatis);
  }

  @BeforeClass
  public static void init() throws Exception {
    setIsBucketFSOptimized(true);
    TestRootedOzoneFileSystem.init();
  }

  /**
   * OFS: Test recursive listStatus on root and volume.
   */
  @Override
  @Ignore("TODO:HDDS-4360")
  public void testListStatusRootAndVolumeRecursive() throws IOException {
  }

  @Test
  public void testRenameDir() throws Exception {
    final String dir = "dir1";
    final Path source = new Path(getBucketPath(), dir);
    final Path dest = new Path(source.toString() + ".renamed");
    // Add a sub-dir to the directory to be moved.
    final Path subdir = new Path(source, "sub_dir1");
    getFs().mkdirs(subdir);
    LOG.info("Created dir {}", subdir);
    LOG.info("Will move {} to {}", source, dest);
    getFs().rename(source, dest);
    assertTrue("Directory rename failed", getFs().exists(dest));
    // Verify that the subdir is also renamed i.e. keys corresponding to the
    // sub-directories of the renamed directory have also been renamed.
    assertTrue("Keys under the renamed directory not renamed",
        getFs().exists(new Path(dest, "sub_dir1")));
    // cleanup
    getFs().delete(dest, true);
  }

  @Test
  public void testRenameFile() throws Exception {
    final String dir = "/dir" + new Random().nextInt(1000);
    Path dirPath = new Path(getBucketPath() +dir);
    getFs().mkdirs(dirPath);

    Path file1Source = new Path(getBucketPath() + dir
        + "/file1_Copy");
    ContractTestUtils.touch(getFs(), file1Source);
    Path file1Destin = new Path(getBucketPath() + dir + "/file1");
    assertTrue("Renamed failed", getFs().rename(file1Source, file1Destin));
    assertTrue("Renamed failed: /dir/file1", getFs().exists(file1Destin));
    FileStatus[] fStatus = getFs().listStatus(dirPath);
    assertEquals("Renamed failed", 1, fStatus.length);
    getFs().delete(getBucketPath(), true);
  }



  /**
   * Rename file to an existed directory.
   */
  @Test
  public void testRenameFileToDir() throws Exception {
    final String dir = "/dir" + new Random().nextInt(1000);
    Path dirPath = new Path(getBucketPath() +dir);
    getFs().mkdirs(dirPath);

    Path file1Destin = new Path(getBucketPath() + dir  + "/file1");
    ContractTestUtils.touch(getFs(), file1Destin);
    Path abcRootPath = new Path(getBucketPath() + "/a/b/c");
    getFs().mkdirs(abcRootPath);
    assertTrue("Renamed failed", getFs().rename(file1Destin, abcRootPath));
    assertTrue("Renamed filed: /a/b/c/file1", getFs().exists(new Path(
        abcRootPath, "file1")));
    getFs().delete(getBucketPath(), true);
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
    final Path dir2SourcePath = new Path(getBucketPath() + dir2);
    getFs().mkdirs(dir2SourcePath);
    final Path destRootPath = new Path(getBucketPath() + root);

    Path file1Source = new Path(getBucketPath() + dir1 + "/file2");
    ContractTestUtils.touch(getFs(), file1Source);

    // rename source directory to its parent directory(destination).
    assertTrue("Rename failed", getFs().rename(dir2SourcePath, destRootPath));
    final Path expectedPathAfterRename =
        new Path(getBucketPath() + root + "/dir2");
    assertTrue("Rename failed",
        getFs().exists(expectedPathAfterRename));

    // rename source file to its parent directory(destination).
    assertTrue("Rename failed", getFs().rename(file1Source, destRootPath));
    final Path expectedFilePathAfterRename =
        new Path(getBucketPath() + root + "/file2");
    assertTrue("Rename failed",
        getFs().exists(expectedFilePathAfterRename));
    getFs().delete(getBucketPath(), true);
  }


}
