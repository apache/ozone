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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
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
 * Unit tests for the head-op (metadata-only) type checks on o3fs
 * ({@link BasicOzoneFileSystem#isDirectory}/{@link BasicOzoneFileSystem#isFile})
 * added in HDDS-15877. Uses a mock adapter so no cluster is required.
 */
public class TestOzoneFileSystemHeadOp {

  private BasicOzoneClientAdapterImpl adapter;
  private BasicOzoneFileSystem fs;

  /** Test FS that injects a mock adapter instead of connecting to OM. */
  private final class MockAdapterFs extends BasicOzoneFileSystem {
    @Override
    protected OzoneClientAdapter createAdapter(ConfigurationSource conf,
        String bucketStr, String volumeStr, String omHost, int omPort) {
      return adapter;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    adapter = mock(BasicOzoneClientAdapterImpl.class);
    fs = new MockAdapterFs();
    fs.initialize(URI.create("o3fs://bucket.vol/"), new OzoneConfiguration());
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
    Path dir = new Path("/dir");

    assertTrue(fs.isDirectory(dir));
    assertFalse(fs.isFile(dir));

    ArgumentCaptor<Boolean> headOp = ArgumentCaptor.forClass(Boolean.class);
    verify(adapter, atLeastOnce()).getFileStatus(
        anyString(), any(URI.class), any(Path.class), anyString(),
        headOp.capture());
    for (Boolean v : headOp.getAllValues()) {
      assertTrue(v, "isDirectory/isFile must request headOp");
    }
  }

  @Test
  public void isFileUsesHeadOp() throws IOException {
    stubStatus(false);
    Path file = new Path("/file");

    assertTrue(fs.isFile(file));
    assertFalse(fs.isDirectory(file));
  }

  @Test
  public void fullGetFileStatusDoesNotUseHeadOp() throws IOException {
    stubStatus(false);
    fs.getFileStatus(new Path("/file"));
    verify(adapter).getFileStatus(anyString(), any(URI.class), any(Path.class),
        anyString(), eq(false));
  }

  @Test
  public void missingPathReturnsFalse() throws IOException {
    // The adapter maps FILE_NOT_FOUND to FileNotFoundException; the FS maps
    // KEY_NOT_FOUND to FileNotFoundException. Both are swallowed as "false".
    stubThrow(new FileNotFoundException("missing"));
    Path missing = new Path("/missing");
    assertFalse(fs.isDirectory(missing));
    assertFalse(fs.isFile(missing));

    stubThrow(new OMException("missing",
        OMException.ResultCodes.KEY_NOT_FOUND));
    assertFalse(fs.isDirectory(missing));
    assertFalse(fs.isFile(missing));
  }

  @Test
  public void otherOMExceptionPropagates() throws IOException {
    stubThrow(new OMException("denied",
        OMException.ResultCodes.PERMISSION_DENIED));
    assertThrows(OMException.class,
        () -> fs.isDirectory(new Path("/x")));
  }

  @Test
  public void plainIOExceptionPropagates() throws IOException {
    stubThrow(new IOException("io"));
    assertThrows(IOException.class,
        () -> fs.isFile(new Path("/x")));
  }
}
