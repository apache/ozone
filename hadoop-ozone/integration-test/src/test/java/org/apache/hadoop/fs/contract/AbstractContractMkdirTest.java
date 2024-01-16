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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test directory operations.
 */
public abstract class AbstractContractMkdirTest extends AbstractFSContractTestBase {

  @Test
  public void testMkDirRmDir() throws Throwable {
    FileSystem fs = getFileSystem();

    Path dir = path("testMkDirRmDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, false);
  }

  @Test
  public void testMkDirRmRfDir() throws Throwable {
    describe("create a directory then recursive delete it");
    FileSystem fs = getFileSystem();
    Path dir = path("testMkDirRmRfDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, true);
  }

  @Test
  public void testNoMkdirOverFile() throws Throwable {
    describe("try to mkdir over a file");
    FileSystem fs = getFileSystem();
    Path path = path("testNoMkdirOverFile");
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    try {
      boolean made = fs.mkdirs(path);
      fail("mkdirs did not fail over a file but returned " + made
            + "; " + ls(path));
    } catch (ParentNotDirectoryException | FileAlreadyExistsException e) {
      //parent is a directory
      handleExpectedException(e);
    } catch (IOException e) {
      //here the FS says "no create"
      handleRelaxedException("mkdirs", "FileAlreadyExistsException", e);
    }
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", path);
    assertDeleted(path, true);
  }

  @Test
  public void testMkdirOverParentFile() throws Throwable {
    describe("try to mkdir where a parent is a file");
    FileSystem fs = getFileSystem();
    Path path = path("testMkdirOverParentFile");
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    Path child = new Path(path, "child-to-mkdir");
    try {
      boolean made = fs.mkdirs(child);
      fail("mkdirs did not fail over a file but returned " + made
           + "; " + ls(path));
    } catch (ParentNotDirectoryException | FileAlreadyExistsException e) {
      //parent is a directory
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("mkdirs", "ParentNotDirectoryException", e);
    }
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", path);
    assertDeleted(path, true);
  }

  @Test
  public void testMkdirSlashHandling() throws Throwable {
    describe("verify mkdir slash handling");
    FileSystem fs = getFileSystem();

    final Path[] paths = new Path[] {
        path("testMkdirSlashHandling/a"), // w/o trailing slash
        path("testMkdirSlashHandling/b/"), // w/ trailing slash
        // unqualified w/o trailing slash
        new Path(getContract().getTestPath() + "/testMkdirSlashHandling/c"),
        // unqualified w/ trailing slash
        new Path(getContract().getTestPath() + "/testMkdirSlashHandling/d/"),
        // unqualified w/ multiple trailing slashes
        new Path(getContract().getTestPath() + "/testMkdirSlashHandling/e///")
    };
    for (Path path : paths) {
      Assertions.assertThat(fs.mkdirs(path)).isTrue();
      assertPathExists(path + " does not exist after mkdirs", path);
      assertIsDirectory(path);
      if (path.toString().endsWith("/")) {
        String s = path.toString().substring(0, path.toString().length() - 1);
        assertIsDirectory(new Path(s));
      }
    }
  }

  @Test
  public void testMkdirsPopulatingAllNonexistentAncestors() throws IOException {
    describe("Verify mkdir will populate all its non-existent ancestors");
    final FileSystem fs = getFileSystem();

    final Path parent = path("testMkdirsPopulatingAllNonexistentAncestors");
    Assertions.assertThat(fs.mkdirs(parent)).isTrue();
    assertPathExists(parent + " should exist before making nested dir", parent);

    Path nested = path(parent + "/a/b/c/d/e/f/g/h/i/j/k/L");
    Assertions.assertThat(fs.mkdirs(nested)).isTrue();
    while (nested != null && !nested.equals(parent) && !nested.isRoot()) {
      assertPathExists(nested + " nested dir should exist", nested);
      nested = nested.getParent();
    }
  }

  @Test
  public void testMkdirsDoesNotRemoveParentDirectories() throws IOException {
    describe("Verify mkdir will make its parent existent");
    final FileSystem fs = getFileSystem();

    final Path parent = path("testMkdirsDoesNotRemoveParentDirectories");
    Assertions.assertThat(fs.mkdirs(parent)).isTrue();

    Path p = parent;
    for (int i = 0; i < 10; i++) {
      Assertions.assertThat(fs.mkdirs(p)).isTrue();
      assertPathExists(p + " should exist after mkdir(" + p + ")", p);
      p = path(p + "/dir-" + i);
    }

    // After mkdirs(sub-directory), its parent directory still exists
    p = p.getParent();
    while (p != null && !p.equals(parent) && !p.isRoot()) {
      assertPathExists("Path " + p + " should exist", p);
      assertIsDirectory(p);
      p = p.getParent();
    }
  }

  @Test
  public void testCreateDirWithExistingDir() throws Exception {
    Path path = path("testCreateDirWithExistingDir");
    final FileSystem fs = getFileSystem();
    assertMkdirs(fs, path);
    assertMkdirs(fs, path);
  }
}
