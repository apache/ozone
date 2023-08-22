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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests {@link DiskCheckUtil} does not incorrectly identify an unhealthy
 * disk or mount point.
 * Tests that it identifies an improperly configured directory mount point.
 *
 */
public class TestDiskCheckUtil {
  @Rule
  public TemporaryFolder tempTestDir = new TemporaryFolder();

  private File testDir;

  @Before
  public void setup() {
    testDir = tempTestDir.getRoot();
  }

  @Test
  public void testPermissions() {
    // Ensure correct test setup before testing the disk check.
    Assert.assertTrue(testDir.canRead());
    Assert.assertTrue(testDir.canWrite());
    Assert.assertTrue(testDir.canExecute());
    Assert.assertTrue(DiskCheckUtil.checkPermissions(testDir));

    // Test failure without read permissiosns.
    Assert.assertTrue(testDir.setReadable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setReadable(true));

    // Test failure without write permissiosns.
    Assert.assertTrue(testDir.setWritable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setWritable(true));

    // Test failure without execute permissiosns.
    Assert.assertTrue(testDir.setExecutable(false));
    Assert.assertFalse(DiskCheckUtil.checkPermissions(testDir));
    Assert.assertTrue(testDir.setExecutable(true));
  }

  @Test
  public void testExistence() {
    // Ensure correct test setup before testing the disk check.
    Assert.assertTrue(testDir.exists());
    Assert.assertTrue(DiskCheckUtil.checkExistence(testDir));

    Assert.assertTrue(testDir.delete());
    Assert.assertFalse(DiskCheckUtil.checkExistence(testDir));
  }

  @Test
  public void testReadWrite() {
    Assert.assertTrue(DiskCheckUtil.checkReadWrite(testDir, testDir, 10));

    // Test file should have been deleted.
    File[] children = testDir.listFiles();
    Assert.assertNotNull(children);
    Assert.assertEquals(0, children.length);
  }
}
