/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

/**
 * Tests {@link DiskCheckUtil} does not incorrectly identify an unhealthy
 * disk or mount point.
 * Tests that it identifies an improperly configured directory mount point.
 *
 */
public class TestDiskCheckUtil {

  @TempDir
  private File testDir;
  
  @Test
  public void testPermissions() {
    // Ensure correct test setup before testing the disk check.
    Assertions.assertTrue(testDir.canRead());
    Assertions.assertTrue(testDir.canWrite());
    Assertions.assertTrue(testDir.canExecute());
    Assertions.assertTrue(DiskCheckUtil.checkPermissions(testDir));

    // Test failure without read permissiosns.
    Assertions.assertTrue(testDir.setReadable(false));
    Assertions.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assertions.assertTrue(testDir.setReadable(true));

    // Test failure without write permissiosns.
    Assertions.assertTrue(testDir.setWritable(false));
    Assertions.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assertions.assertTrue(testDir.setWritable(true));

    // Test failure without execute permissiosns.
    Assertions.assertTrue(testDir.setExecutable(false));
    Assertions.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assertions.assertTrue(testDir.setExecutable(true));
  }

  @Test
  public void testExistence() {
    // Ensure correct test setup before testing the disk check.
    Assertions.assertTrue(testDir.exists());
    Assertions.assertTrue(DiskCheckUtil.checkExistence(testDir));

    Assertions.assertTrue(testDir.delete());
    Assertions.assertFalse(DiskCheckUtil.checkExistence(testDir));
  }

  @Test
  public void testReadWrite() {
    Assertions.assertTrue(DiskCheckUtil.checkReadWrite(testDir, testDir, 10));

    // Test file should have been deleted.
    File[] children = testDir.listFiles();
    Assertions.assertNotNull(children);
    Assertions.assertEquals(0, children.length);
  }
}
