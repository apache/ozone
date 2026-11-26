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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link BasicRootedOzoneFileSystem#rename} on an OM that rejects a rename requiring a
 * FILE_SYSTEM_OPTIMIZED bucket on other layouts (HDDS-16951): an FSO rename is one RenameKey with no InfoBucket, and
 * every other case falls back to the path that fetches the bucket. Uses a mock adapter so no cluster is required.
 */
public class TestRootedOzoneFileSystemRename {

  private static final Path SRC = new Path("/vol/bucket/dir/file");
  private static final Path DST = new Path("/vol/bucket/dir/file2");
  private static final String SRC_KEY_PATH = "vol/bucket/dir/file";
  private static final String DST_KEY_PATH = "vol/bucket/dir/file2";

  private BasicRootedOzoneClientAdapterImpl adapter;
  private BasicRootedOzoneFileSystem fs;
  private OzoneBucket bucket;

  /** Test FS that injects a mock adapter instead of connecting to OM. */
  private final class MockAdapterFs extends BasicRootedOzoneFileSystem {
    @Override
    protected OzoneClientAdapter createAdapter(ConfigurationSource conf, String omHost, int omPort) {
      return adapter;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class);
    fs = new MockAdapterFs();
    fs.initialize(URI.create("ofs://om/"), new OzoneConfiguration());
    bucket = mock(OzoneBucket.class);
    when(adapter.getBucket(any(OFSPath.class), anyBoolean())).thenReturn(bucket);
    when(adapter.isRenameRequiringFileSystemOptimizedSupported()).thenReturn(true);
  }

  private void failFsoRename(ResultCodes code) throws IOException {
    doThrow(new OMException("failed", code)).when(adapter).renameRequiringFileSystemOptimized(anyString(), anyString());
  }

  @Test
  public void fsoRenameIsOneRenameKeyWithoutInfoBucket() throws IOException {
    assertTrue(fs.rename(SRC, DST));

    verify(adapter).renameRequiringFileSystemOptimized(SRC_KEY_PATH, DST_KEY_PATH);
    verify(adapter, never()).getBucket(any(OFSPath.class), anyBoolean());
    verify(adapter, never()).rename(any(OzoneBucket.class), anyString(), anyString());
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {"KEY_ALREADY_EXISTS", "KEY_RENAME_ERROR", "KEY_NOT_FOUND"})
  public void fsoRenameFailureReturnsFalse(ResultCodes code) throws IOException {
    failFsoRename(code);

    assertFalse(fs.rename(SRC, DST));
    verify(adapter, never()).getBucket(any(OFSPath.class), anyBoolean());
  }

  @Test
  public void otherFsoRenameErrorIsRethrown() throws IOException {
    failFsoRename(ResultCodes.PERMISSION_DENIED);

    OMException e = assertThrows(OMException.class, () -> fs.rename(SRC, DST));
    assertEquals(ResultCodes.PERMISSION_DENIED, e.getResult());
    verify(adapter, never()).getBucket(any(OFSPath.class), anyBoolean());
  }

  @Test
  public void legacyBucketFallsBackToTheIteratingRename() throws IOException {
    failFsoRename(ResultCodes.NOT_SUPPORTED_OPERATION);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.LEGACY);
    // The LEGACY path starts by looking up the source.
    when(adapter.getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(), anyBoolean()))
        .thenThrow(new FileNotFoundException("no source"));

    assertFalse(fs.rename(SRC, DST));
    verify(adapter).getBucket(any(OFSPath.class), anyBoolean());
    verify(adapter).getFileStatus(anyString(), any(URI.class), any(Path.class), anyString(), anyBoolean());
    verify(adapter, never()).rename(any(OzoneBucket.class), anyString(), anyString());
  }

  @Test
  public void objectStoreBucketFallsBackToTheClientSideRejection() throws IOException {
    failFsoRename(ResultCodes.NOT_SUPPORTED_OPERATION);
    IllegalArgumentException rejection = new IllegalArgumentException("Bucket: bucket has layout: OBJECT_STORE");
    when(adapter.getBucket(any(OFSPath.class), anyBoolean())).thenThrow(rejection);

    assertSame(rejection, assertThrows(IllegalArgumentException.class, () -> fs.rename(SRC, DST)));
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {"BUCKET_NOT_FOUND", "VOLUME_NOT_FOUND"})
  public void missingBucketFailsAsBefore(ResultCodes code) throws IOException {
    failFsoRename(code);
    OMException fromGetBucket = new OMException("missing", code);
    when(adapter.getBucket(any(OFSPath.class), anyBoolean())).thenThrow(fromGetBucket);

    assertSame(fromGetBucket, assertThrows(OMException.class, () -> fs.rename(SRC, DST)));
  }

  @Test
  public void olderOmFetchesTheBucket() throws IOException {
    when(adapter.isRenameRequiringFileSystemOptimizedSupported()).thenReturn(false);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertTrue(fs.rename(SRC, DST));
    verify(adapter).getBucket(any(OFSPath.class), anyBoolean());
    verify(adapter).rename(bucket, SRC_KEY_PATH, DST_KEY_PATH);
    verify(adapter, never()).renameRequiringFileSystemOptimized(anyString(), anyString());
  }

  /** The adapter sends the rename with the field set and gates it on the OM version. */
  @Test
  public void adapterSendsTheFlaggedRenameOnlyToANewOm() throws Exception {
    BasicRootedOzoneClientAdapterImpl realAdapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    ClientProtocol proxy = mock(ClientProtocol.class);
    setField(realAdapter, "proxy", proxy);
    setField(realAdapter, "config", new OzoneConfiguration());

    realAdapter.renameRequiringFileSystemOptimized(SRC_KEY_PATH, DST_KEY_PATH);
    verify(proxy).renameKey("vol", "bucket", "dir/file", "dir/file2", true);

    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.RENAME_KEY_REJECTS_NON_FSO);
    assertTrue(realAdapter.isRenameRequiringFileSystemOptimizedSupported());
    // An OM finalized for ZDU, or at a later version without this change, must not be trusted to reject.
    for (OzoneManagerVersion older : new OzoneManagerVersion[] {
        OzoneManagerVersion.ZDU, OzoneManagerVersion.GET_KEY_INFO_REJECTS_OBS}) {
      when(proxy.getOmVersion()).thenReturn(older);
      assertFalse(realAdapter.isRenameRequiringFileSystemOptimizedSupported(), older.name());
    }
  }

  private static void setField(BasicRootedOzoneClientAdapterImpl target, String name, Object value) throws Exception {
    Field field = BasicRootedOzoneClientAdapterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
