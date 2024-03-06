/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.Path;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.fail;

/**
 * Test creating files, overwrite options &c.
 */
public abstract class AbstractContractDeleteTest extends
                                                 AbstractFSContractTestBase {

  @Test
  public void testDeleteEmptyDirNonRecursive() throws Throwable {
    Path path = path("testDeleteEmptyDirNonRecursive");
    mkdirs(path);
    assertDeleted(path, false);
  }

  @Test
  public void testDeleteEmptyDirRecursive() throws Throwable {
    Path path = path("testDeleteEmptyDirRecursive");
    mkdirs(path);
    assertDeleted(path, true);
  }

  @Test
  public void testDeleteNonexistentPathRecursive() throws Throwable {
    Path path = path("testDeleteNonexistentPathRecursive");
    assertPathDoesNotExist("leftover", path);
    ContractTestUtils.rejectRootOperation(path);
    Assertions.assertThat(getFileSystem().delete(path, true))
        .withFailMessage("Returned true attempting to recursively delete"
            + " a nonexistent path " + path)
        .isFalse();
  }

  @Test
  public void testDeleteNonexistentPathNonRecursive() throws Throwable {
    Path path = path("testDeleteNonexistentPathNonRecursive");
    assertPathDoesNotExist("leftover", path);
    ContractTestUtils.rejectRootOperation(path);
    Assertions.assertThat(getFileSystem().delete(path, false))
        .withFailMessage("Returned true attempting to non recursively delete"
            + " a nonexistent path " + path)
        .isFalse();
  }

  @Test
  public void testDeleteNonEmptyDirNonRecursive() throws Throwable {
    Path path = path("testDeleteNonEmptyDirNonRecursive");
    mkdirs(path);
    Path file = new Path(path, "childfile");
    ContractTestUtils.writeTextFile(getFileSystem(), file, "goodbye, world",
                                    true);
    try {
      ContractTestUtils.rejectRootOperation(path);
      boolean deleted = getFileSystem().delete(path, false);
      fail("non recursive delete should have raised an exception," +
           " but completed with exit code " + deleted);
    } catch (IOException expected) {
      //expected
      handleExpectedException(expected);
    }
    assertIsDirectory(path);
  }

  @Test
  public void testDeleteNonEmptyDirRecursive() throws Throwable {
    Path path = path("testDeleteNonEmptyDirRecursive");
    mkdirs(path);
    Path file = new Path(path, "childfile");
    ContractTestUtils.writeTextFile(getFileSystem(), file, "goodbye, world",
                                    true);
    assertDeleted(path, true);
    assertPathDoesNotExist("not deleted", file);
  }

  @Test
  public void testDeleteDeepEmptyDir() throws Throwable {
    mkdirs(path("testDeleteDeepEmptyDir/d1/d2/d3/d4"));
    assertDeleted(path("testDeleteDeepEmptyDir/d1/d2/d3"), true);

    assertPathDoesNotExist(
        "not deleted", path("testDeleteDeepEmptyDir/d1/d2/d3/d4"));
    assertPathDoesNotExist(
        "not deleted", path("testDeleteDeepEmptyDir/d1/d2/d3"));
    assertPathExists("parent dir is deleted",
        path("testDeleteDeepEmptyDir/d1/d2"));
  }

  @Test
  public void testDeleteSingleFile() throws Throwable {
    // Test delete of just a file
    Path path = path("testDeleteSingleFile/d1/d2");
    mkdirs(path);
    Path file = new Path(path, "childfile");
    ContractTestUtils.writeTextFile(getFileSystem(), file,
        "single file to be deleted.", true);
    assertPathExists("single file not created", file);
    assertDeleted(file, false);
  }
}
