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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link BasicRootedOzoneClientAdapterImpl#readFile}, covering the read without InfoBucket added by
 * HDDS-16948 and the fallback used when the OM predates {@link OzoneManagerVersion#GET_KEY_INFO_REJECTS_OBS}.
 * Uses a partial mock so no OM connection is required.
 */
public class TestBasicRootedOzoneClientAdapterReadFile {

  private static final String FILE_PATH = "/vol/bucket/dir/file";

  private BasicRootedOzoneClientAdapterImpl adapter;
  private ClientProtocol proxy;
  private OzoneBucket bucket;
  private InputStream stream;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    proxy = mock(ClientProtocol.class);
    bucket = mock(OzoneBucket.class);
    stream = mock(InputStream.class);
    OzoneInputStream ozoneStream = new OzoneInputStream(stream);

    when(proxy.readFile("vol", "bucket", "dir/file", true)).thenReturn(ozoneStream);
    when(proxy.getBucketDetails("vol", "bucket")).thenReturn(bucket);
    when(bucket.getName()).thenReturn("bucket");
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(bucket.readFile("dir/file")).thenReturn(ozoneStream);

    setField("proxy", proxy);
    setField("objectStore", mock(ObjectStore.class));
    setField("config", new OzoneConfiguration());

    // Default to an OM that performs the server-side OBJECT_STORE rejection.
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.GET_KEY_INFO_REJECTS_OBS);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = BasicRootedOzoneClientAdapterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(adapter, value);
  }

  private void failRead(OMException e) throws IOException {
    when(proxy.readFile("vol", "bucket", "dir/file", true)).thenThrow(e);
  }

  @Test
  public void readIsTheOnlyOmCall() throws IOException {
    assertSame(stream, adapter.readFile(FILE_PATH));

    verify(proxy).readFile("vol", "bucket", "dir/file", true);
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @Test
  public void objectStoreRejectionSurfacesAsIllegalArgumentException() throws IOException {
    String message = "Bucket: bucket has layout: OBJECT_STORE, which does not support file system semantics.";
    failRead(new OMException(message, ResultCodes.NOT_SUPPORTED_OPERATION));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> adapter.readFile(FILE_PATH));
    assertEquals(message, e.getMessage());
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {"KEY_NOT_FOUND", "FILE_NOT_FOUND", "NOT_A_FILE"})
  public void missingFileSurfacesAsFileNotFoundException(ResultCodes code) throws IOException {
    failRead(new OMException("missing", code));

    FileNotFoundException e = assertThrows(FileNotFoundException.class, () -> adapter.readFile(FILE_PATH));
    assertThat(e.getMessage()).startsWith(code.name());
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {"BUCKET_NOT_FOUND", "VOLUME_NOT_FOUND", "PERMISSION_DENIED"})
  public void otherErrorsAreRethrownWithoutFetchingTheBucket(ResultCodes code) throws IOException {
    // A missing bucket or volume fails as before, with the same result code that InfoBucket reported.
    failRead(new OMException("failed", code));

    OMException e = assertThrows(OMException.class, () -> adapter.readFile(FILE_PATH));
    assertEquals(code, e.getResult());
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @ParameterizedTest
  @EnumSource(value = OzoneManagerVersion.class, names = {"LOOKUP_FILE_REJECTS_OBS", "ZDU", "LIST_STATUS_REJECTS_OBS"})
  public void olderOmFallsBackToClientSideBucketCheck(OzoneManagerVersion version) throws IOException {
    // An OM finalized for ZDU reports ZDU, or a later version, even without this change, so only an OM at or above
    // GET_KEY_INFO_REJECTS_OBS is trusted to reject OBJECT_STORE.
    when(proxy.getOmVersion()).thenReturn(version);

    assertSame(stream, adapter.readFile(FILE_PATH));

    verify(proxy).getBucketDetails("vol", "bucket");
    verify(bucket).readFile("dir/file");
    verify(proxy, never()).readFile(anyString(), anyString(), anyString(), anyBoolean());
  }

  @Test
  public void olderOmRejectsObsBucketClientSide() throws IOException {
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.LIST_STATUS_REJECTS_OBS);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> adapter.readFile(FILE_PATH));
    assertThat(e.getMessage()).contains("Bucket: bucket has layout: OBJECT_STORE");
    verify(bucket, never()).readFile(anyString());
  }

  @Test
  public void pathWithoutBucketIsFileNotFound() throws IOException {
    assertThrows(FileNotFoundException.class, () -> adapter.readFile("/vol"));

    verify(proxy, never()).readFile(anyString(), anyString(), anyString(), anyBoolean());
  }
}
