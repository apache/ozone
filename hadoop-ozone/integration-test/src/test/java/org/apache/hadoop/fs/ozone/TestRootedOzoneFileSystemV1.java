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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Assert;
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

  /**
   * Cleanup keyTable and directoryTable explicitly as FS delete operation
   * is not yet supported.
   * Fails if the (a) parent of dst does not exist or (b) parent is a file.
   */
  @Override
  @Test
  public void testRenameDestinationParentDoesntExist() throws Exception {
    final String root = "/root_dir";
    final String dir1 = root + "/dir1";
    final String dir2 = dir1 + "/dir2";
    final Path dir2SourcePath = new Path(getBucketPath() + dir2);
    getFs().mkdirs(dir2SourcePath);
    // (a) parent of dst does not exist.  /root_dir/b/c
    final Path destinPath = new Path(getBucketPath()
        + root + "/b/c");

    // rename should fail and return false
    Assert.assertFalse(getFs().rename(dir2SourcePath, destinPath));
    // (b) parent of dst is a file. /root_dir/file1/c
    Path filePath = new Path(getBucketPath() + root + "/file1");
    ContractTestUtils.touch(getFs(), filePath);
    Path newDestinPath = new Path(filePath, "c");
    // rename should fail and return false
    Assert.assertFalse(getFs().rename(dir2SourcePath, newDestinPath));
  }

  /**
   *  Cannot rename a directory to its own subdirectory.
   */
  @Override
  @Test
  public void testRenameDirToItsOwnSubDir() throws Exception {
    final String root = "/root";
    final String dir1 = root + "/dir1";
    final Path dir1Path = new Path(getBucketPath() + dir1);
    // Add a sub-dir1 to the directory to be moved.
    final Path subDir1 = new Path(dir1Path, "sub_dir1");
    getFs().mkdirs(subDir1);
    LOG.info("Created dir1 {}", subDir1);

    final Path sourceRoot = new Path(getBucketPath() + root);
    LOG.info("Rename op-> source:{} to destin:{}", sourceRoot, subDir1);
    //  rename should fail and return false
    Assert.assertFalse(getFs().rename(sourceRoot, subDir1));
  }

}
