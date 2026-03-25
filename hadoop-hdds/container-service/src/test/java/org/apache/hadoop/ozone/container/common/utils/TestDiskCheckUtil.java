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

package org.apache.hadoop.ozone.container.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.io.File;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ratis.util.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.MockedStatic;

/**
 * Tests {@link DiskCheckUtil} does not incorrectly identify an unhealthy
 * disk or mount point.
 * Tests that it identifies an improperly configured directory mount point.
 *
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDiskCheckUtil {

  @TempDir
  private File testDir;
  
  @Test
  public void testPermissions() {
    // Ensure correct test setup before testing the disk check.
    assertTrue(testDir.canRead());
    assertTrue(testDir.canWrite());
    assertTrue(testDir.canExecute());
    assertTrue(DiskCheckUtil.checkPermissions(testDir));

    // Test failure without read permissiosns.
    assertTrue(testDir.setReadable(false));
    assertFalse(DiskCheckUtil.checkPermissions(testDir));
    assertTrue(testDir.setReadable(true));

    // Test failure without write permissiosns.
    assertTrue(testDir.setWritable(false));
    assertFalse(DiskCheckUtil.checkPermissions(testDir));
    assertTrue(testDir.setWritable(true));

    // Test failure without execute permissiosns.
    assertTrue(testDir.setExecutable(false));
    assertFalse(DiskCheckUtil.checkPermissions(testDir));
    assertTrue(testDir.setExecutable(true));
  }

  @Test
  public void testExistence() {
    // Ensure correct test setup before testing the disk check.
    assertTrue(testDir.exists());
    assertTrue(DiskCheckUtil.checkExistence(testDir));

    assertTrue(testDir.delete());
    assertFalse(DiskCheckUtil.checkExistence(testDir));
  }

  @Test
  public void testReadWrite() {
    assertTrue(DiskCheckUtil.checkReadWrite(testDir, testDir, 10));

    // Test file should have been deleted.
    File[] children = testDir.listFiles();
    assertNotNull(children);
    assertEquals(0, children.length);
  }

  @Test
  public void testWriteFailureDueToTooManyOpenFiles() {
    try (MockedStatic<FileUtils> mockService = mockStatic(FileUtils.class)) {

      mockService.when(() -> FileUtils.newOutputStreamForceAtClose(any(File.class), any(OpenOption[].class)))
          .thenThrow(new ExceptionInInitializerError("java.io.IOException: Too many open files"));

      String path = "/Volumes/DiskFullTest/disk-check-c967569c";
      assertThrows(ExceptionInInitializerError.class,
          () -> FileUtils.newOutputStreamForceAtClose(new File(path), new OpenOption[2]));

      // Test that checkReadWrite returns true for the too many open file case
      boolean result = DiskCheckUtil.checkReadWrite(testDir, testDir, 1024);
      assertTrue(result, "checkReadWrite should return true when too many open files");
    }
  }

  @Test
  public void testReadFailureDueToTooManyOpenFiles() {
    try (MockedStatic<Files> mockService = mockStatic(Files.class)) {

      String path = "/Volumes/DiskFullTest/disk-check-c967569c";
      mockService.when(() -> Files.newInputStream(any(Path.class), any(OpenOption[].class)))
          .thenThrow(new FileSystemException(path + ": Too many open files"));

      assertThrows(FileSystemException.class,
          () -> Files.newInputStream(Paths.get(path), new OpenOption[2]));

      // Test that checkReadWrite returns true for the too many open file case
      boolean result = DiskCheckUtil.checkReadWrite(testDir, testDir, 1024);
      assertTrue(result, "checkReadWrite should return true when too many open files");
    }
  }
}
