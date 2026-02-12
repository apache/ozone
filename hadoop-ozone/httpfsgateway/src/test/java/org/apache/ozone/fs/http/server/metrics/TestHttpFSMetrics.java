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

package org.apache.ozone.fs.http.server.metrics;

import static org.apache.ozone.lib.service.hadoop.FileSystemAccessService.FILE_SYSTEM_SERVICE_CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.fs.http.server.FSOperations;
import org.apache.ozone.fs.http.server.HttpFSServerWebApp;
import org.apache.ozone.lib.service.FileSystemAccess;
import org.apache.ozone.lib.service.hadoop.FileSystemAccessService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for HttpFSServerMetrics.
 */
public class TestHttpFSMetrics {
  private static FileSystem mockFs = mock(FileSystem.class);
  private static FSDataOutputStream fsDataOutputStream = mock(FSDataOutputStream.class);

  private HttpFSServerMetrics metrics;
  private Configuration conf;
  private UserGroupInformation ugi;
  private FileSystemAccess fsAccess;

  @BeforeAll
  static void init(@TempDir File dir) throws Exception {
    File tempDir = new File(dir, "temp");
    assertTrue(tempDir.mkdirs());
    File logDir = new File(dir, "log");
    assertTrue(logDir.mkdirs());
    File confDir = new File(dir, "conf");
    assertTrue(confDir.mkdirs());
    System.setProperty("httpfs.home.dir", dir.getAbsolutePath());
  }

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(FILE_SYSTEM_SERVICE_CREATED, true);

    HttpFSServerWebApp webApp = new HttpFSServerWebApp();
    webApp.init();
    webApp.setService(MockFileSystemAccessService.class);

    fsAccess = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    metrics = HttpFSServerWebApp.getMetrics();
    ugi = UserGroupInformation.createUserForTesting("testuser", new String[] {"testgroup"});
  }

  @AfterEach
  public void tearDown() {
    if (metrics != null) {
      metrics.shutdown();
    }
    HttpFSServerWebApp.get().destroy();
  }

  @Test
  public void testFsCreate() throws Exception {
    long initialCreateOps = metrics.getOpsCreate();
    long initialBytesWritten = metrics.getBytesWritten();

    FSOperations.FSCreate createOp = new FSOperations.FSCreate(
        new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)),
        "/test.txt",
        (short) 0644,
        true,
        (short) 3,
        1024L,
        (short) 0644);

    when(mockFs.create(isA(Path.class), isA(FsPermission.class), isA(Boolean.class), isA(Integer.class),
        isA(Short.class), isA(Long.class), any())).thenReturn(fsDataOutputStream);
    fsAccess.execute(ugi.getShortUserName(), conf, createOp);

    assertEquals(initialCreateOps + 1, metrics.getOpsCreate());
    assertEquals(initialBytesWritten + 4, metrics.getBytesWritten());
  }

  @Test
  public void testFsAppend() throws Exception {
    long initialAppendOps = metrics.getOpsAppend();
    long initialBytesWritten = metrics.getBytesWritten();

    FSOperations.FSAppend appendOp = new FSOperations.FSAppend(
        new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8)),
        "/test.txt");

    when(mockFs.append(isA(Path.class), isA(Integer.class))).thenReturn(fsDataOutputStream);
    fsAccess.execute(ugi.getShortUserName(), conf, appendOp);

    assertEquals(initialAppendOps + 1, metrics.getOpsAppend());
    assertEquals(initialBytesWritten + 4, metrics.getBytesWritten());
  }

  /**
   * Mock FileSystemAccessService.
   */
  public static class MockFileSystemAccessService extends FileSystemAccessService {
    @Override
    protected FileSystem createFileSystem(Configuration namenodeConf) throws IOException {
      return mockFs;
    }

    @Override
    protected void closeFileSystem(FileSystem fs) throws IOException {
      // do nothing
    }
  }
}
