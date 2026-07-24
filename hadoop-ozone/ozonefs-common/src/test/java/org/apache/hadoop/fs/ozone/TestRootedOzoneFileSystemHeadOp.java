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

package org.apache.hadoop.fs.ozone;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for the head-op (metadata-only) type checks on OFS
 * ({@link BasicRootedOzoneFileSystem#isDirectory}/{@link
 * BasicRootedOzoneFileSystem#isFile}) added in HDDS-15678. Uses a mock adapter
 * so no cluster is required.
 */
public class TestRootedOzoneFileSystemHeadOp {

  private BasicRootedOzoneClientAdapterImpl adapter;
  private BasicRootedOzoneFileSystem fs;

  /** Test FS that injects a mock adapter instead of connecting to OM. */
  private final class MockAdapterFs extends BasicRootedOzoneFileSystem {
    @Override
    protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
        String omHost, int omPort) {
      return adapter;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class);
    fs = new MockAdapterFs();
    fs.initialize(URI.create("ofs://om/"), new OzoneConfiguration());
  }

  private static FileStatusAdapter status(Path path, boolean isDir) {
    return new FileStatusAdapter(0L, 0L, path, isDir, (short) 3, 0L, 0L, 0L,
        (short) 0, "user", "group", null, new BlockLocation[0], false, false);
  }

  private void stubStatus(boolean isDir) throws IOException {
    when(adapter.getFileStatus(anyString(), any(URI.class), any(Path.class),
        anyString(), anyBoolean()))
        .thenAnswer(inv -> status(inv.getArgument(2), isDir));
  }

  private void stubThrow(IOException e) throws IOException {
    when(adapter.getFileStatus(anyString(), any(URI.class), any(Path.class),
        anyString(), anyBoolean())).thenThrow(e);
  }

  @Test
  public void isDirectoryUsesHeadOp() throws IOException {
    stubStatus(true);
    Path dir = new Path("/vol/bucket/dir");

    assertTrue(fs.isDirectory(dir));
    assertFalse(fs.isFile(dir));

    ArgumentCaptor<Boolean> headOp = ArgumentCaptor.forClass(Boolean.class);
    verify(adapter, org.mockito.Mockito.atLeastOnce()).getFileStatus(
        anyString(), any(URI.class), any(Path.class), anyString(),
        headOp.capture());
    for (Boolean v : headOp.getAllValues()) {
      assertTrue(v, "isDirectory/isFile must request headOp");
    }
  }

  @Test
  public void isFileUsesHeadOp() throws IOException {
    stubStatus(false);
    Path file = new Path("/vol/bucket/file");

    assertTrue(fs.isFile(file));
    assertFalse(fs.isDirectory(file));
  }

  @Test
  public void fullGetFileStatusDoesNotUseHeadOp() throws IOException {
    stubStatus(false);
    fs.getFileStatus(new Path("/vol/bucket/file"));
    verify(adapter).getFileStatus(anyString(), any(URI.class), any(Path.class),
        anyString(), eq(false));
  }

  @Test
  public void missingPathReturnsFalse() throws IOException {
    // Each *_NOT_FOUND result is mapped to FileNotFoundException and swallowed.
    for (OMException.ResultCodes code : new OMException.ResultCodes[] {
        OMException.ResultCodes.KEY_NOT_FOUND,
        OMException.ResultCodes.BUCKET_NOT_FOUND,
        OMException.ResultCodes.VOLUME_NOT_FOUND}) {
      stubThrow(new OMException("not found", code));
      Path missing = new Path("/vol/bucket/missing");
      assertFalse(fs.isDirectory(missing));
      assertFalse(fs.isFile(missing));
    }
  }

  @Test
  public void nonExistenceOMExceptionPropagates() throws IOException {
    stubThrow(new OMException("denied",
        OMException.ResultCodes.PERMISSION_DENIED));
    assertThrows(OMException.class,
        () -> fs.isDirectory(new Path("/vol/bucket/x")));
  }

  @Test
  public void plainIOExceptionPropagates() throws IOException {
    stubThrow(new IOException("io"));
    assertThrows(IOException.class,
        () -> fs.isFile(new Path("/vol/bucket/x")));
  }

  @Test
  public void distCpNonePathReturnsFalse() throws IOException {
    // Key "NONE" is rejected before any RPC.
    assertFalse(fs.isDirectory(new Path("/NONE")));
  }

  /**
   * The OzoneClientAdapter headOp overload has a default that delegates to the
   * 4-arg method (used by the non-rooted o3fs adapter, which keeps full status).
   */
  @Test
  public void adapterHeadOpDefaultDelegates() throws IOException {
    OzoneClientAdapter mockAdapter =
        mock(OzoneClientAdapter.class, CALLS_REAL_METHODS);
    URI uri = URI.create("ofs://om/");
    Path path = new Path("/vol/bucket/file");
    FileStatusAdapter expected = status(path, false);
    doReturn(expected).when(mockAdapter).getFileStatus("k", uri, path, "user");

    assertSame(expected,
        mockAdapter.getFileStatus("k", uri, path, "user", true));
    verify(mockAdapter).getFileStatus("k", uri, path, "user");
  }
}
