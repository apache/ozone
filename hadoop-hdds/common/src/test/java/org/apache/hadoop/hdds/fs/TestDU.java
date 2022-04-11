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
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.ozone.OzoneConsts.KB;
import static org.apache.ozone.test.GenericTestUtils.getTestDir;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Tests for {@link DU}.
 */
public class TestDU {

  private static final File DIR = getTestDir(TestDU.class.getSimpleName());

  @Before
  public void setUp() {
    assumeFalse(Shell.WINDOWS);
    FileUtil.fullyDelete(DIR);
    assertTrue(DIR.mkdirs());
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(DIR);
  }

  static void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g. ZFS)
    // can't compress the file
    Random random = new Random();
    byte[] data = new byte[size];
    random.nextBytes(data);

    assumeTrue(newFile.createNewFile());
    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);

    file.getFD().sync();
    file.close();
  }

  /**
   * Verify that du returns expected used space for a file.
   * We assume here that if a file system crates a file of size
   * that is a multiple of the block size in this file system,
   * then the used size for the file will be exactly that size.
   * This is true for most file systems.
   */
  @Test
  public void testGetUsed() throws Exception {
    final long writtenSize = 32 * KB;
    File file = new File(DIR, "data");
    createFile(file, (int) writtenSize);

    SpaceUsageSource du = new DU(file);
    long duSize = du.getUsedSpace();

    assertFileSize(writtenSize, duSize);
  }

  @Test
  public void testExcludePattern() throws IOException {
    createFile(new File(DIR, "include.txt"), (int) (4 * KB));
    createFile(new File(DIR, "exclude.tmp"), (int) (100 * KB));
    SpaceUsageSource du = new DU(DIR, "*.tmp");

    long usedSpace = du.getUsedSpace();

    assertFileSize(4*KB, usedSpace);
  }

  private static void assertFileSize(long expected, long actual) {
    // Allow for extra 8K on-disk slack for local file systems
    // that may store additional file metadata (eg ext attrs).
    final long max = expected + 8 * KB;
    assertTrue(expected <= actual && actual <= max, () ->
        String.format(
            "Invalid on-disk size: %d, expected to be in [%d, %d]",
            actual, expected, max));
  }

}
