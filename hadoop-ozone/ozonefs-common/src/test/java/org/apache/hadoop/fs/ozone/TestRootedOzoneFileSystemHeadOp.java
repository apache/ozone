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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
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

  private static final BlockLocation[] BLOCKS = {
      new BlockLocation(new String[] {"dn1:9859"}, new String[] {"dn1"}, 0, 100),
      new BlockLocation(new String[] {"dn2:9859"}, new String[] {"dn2"}, 100, 100),
      new BlockLocation(new String[] {"dn3:9859"}, new String[] {"dn3"}, 200, 50),
  };

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
  public void getFileStatusUsesHeadOp() throws IOException {
    stubStatus(false);
    FileStatus status = fs.getFileStatus(new Path("/vol/bucket/file"));
    assertFalse(status instanceof LocatedFileStatus);
    verify(adapter).getFileStatus(anyString(), any(URI.class), any(Path.class),
        anyString(), eq(true));
  }

  /** A 250 byte file: OM returns its block locations only for a full status. */
  private void stubFileWithBlocks() throws IOException {
    when(adapter.getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(), anyBoolean()))
        .thenAnswer(inv -> new FileStatusAdapter(250L, 750L, inv.getArgument(2), false, (short) 3, 100L, 0L, 0L,
            (short) 0, "user", "group", null, inv.<Boolean>getArgument(4) ? new BlockLocation[0] : BLOCKS,
            false, false));
  }

  private void verifyStatusCalls(int headOps, int fullStatuses) throws IOException {
    verify(adapter, times(headOps)).getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(),
        eq(true));
    verify(adapter, times(fullStatuses)).getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(),
        eq(false));
  }

  @Test
  public void getFileStatusKeepsAttributes() throws IOException {
    when(adapter.getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(), anyBoolean()))
        .thenAnswer(inv -> new FileStatusAdapter(250L, 750L, inv.getArgument(2), false, (short) 3, 100L, 7L, 7L,
            (short) 0, "owner", "group", null, new BlockLocation[0], true, true));

    FileStatus status = fs.getFileStatus(new Path("/vol/bucket/file"));
    assertFalse(status instanceof LocatedFileStatus);
    assertEquals(250, status.getLen());
    assertEquals(7, status.getModificationTime());
    assertEquals("owner", status.getOwner());
    assertTrue(status.isFile());
    assertTrue(status.isEncrypted());
    assertTrue(status.isErasureCoded());
  }

  @Test
  public void getFileBlockLocationsOfStatusFetchesLocations() throws IOException {
    stubFileWithBlocks();
    FileStatus status = fs.getFileStatus(new Path("/vol/bucket/file"));

    assertThat(fs.getFileBlockLocations(status, 0, 250)).containsExactly(BLOCKS);
    assertThat(fs.getFileBlockLocations(status, 150, 10)).containsExactly(BLOCKS[1]);
    verifyStatusCalls(1, 2);
  }

  @Test
  public void getFileBlockLocationsOfPathUsesOneRpc() throws IOException {
    stubFileWithBlocks();

    assertThat(fs.getFileBlockLocations(new Path("/vol/bucket/file"), 100, 150)).containsExactly(BLOCKS[1], BLOCKS[2]);
    verifyStatusCalls(0, 1);
  }

  @Test
  public void getFileBlockLocationsWithoutBlocksInRangeDoesNotFetch() throws IOException {
    stubFileWithBlocks();
    FileStatus status = fs.getFileStatus(new Path("/vol/bucket/file"));
    FileStatus empty = new FileStatus(0, false, 3, 100, 0, status.getPath());
    FileStatus dir = new FileStatus(0, true, 0, 0, 0, status.getPath());

    assertThat(fs.getFileBlockLocations(status, 250, 10)).isEmpty();
    assertThat(fs.getFileBlockLocations(empty, 0, 10)).isEmpty();
    assertThat(fs.getFileBlockLocations(dir, 0, 10)).isEmpty();
    assertNull(fs.getFileBlockLocations((FileStatus) null, 0, 10));
    assertThrows(IllegalArgumentException.class, () -> fs.getFileBlockLocations(status, -1, 10));
    // a status from listStatus keeps the placeholder location, so a listing that asks for the locations of every file
    // (like Spark) does not pay an RPC per file
    FileStatus listed = new FileStatus(250, false, 3, 100, 0, status.getPath());
    BlockLocation[] placeholder = fs.getFileBlockLocations(listed, 0, 250);
    assertThat(placeholder).hasSize(1);
    assertThat(placeholder[0].getHosts()).containsExactly("localhost");
    verifyStatusCalls(1, 0);
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
