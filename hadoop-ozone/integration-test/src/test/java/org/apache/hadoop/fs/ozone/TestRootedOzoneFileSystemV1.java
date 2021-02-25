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
  }

}
