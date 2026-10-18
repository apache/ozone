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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BasicRootedOzoneClientAdapterImpl#getFileChecksum},
 * covering the single-RPC LookupFile path added by HDDS-15951, the LookupKey
 * fallback used when the OM predates
 * {@link OzoneManagerVersion#LOOKUP_FILE_REJECTS_OBS}, and the mapping of OM
 * result codes onto file system exceptions. Uses a partial mock so no OM
 * connection is required.
 */
public class TestBasicRootedOzoneClientAdapterGetFileChecksum {

  private static final String KEY_PATH = "/vol/bucket/key";

  private BasicRootedOzoneClientAdapterImpl adapter;
  private ClientProtocol proxy;
  private ObjectStore objectStore;
  private OzoneManagerProtocol omClient;
  private OzoneBucket bucket;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    proxy = mock(ClientProtocol.class);
    objectStore = mock(ObjectStore.class);
    omClient = mock(OzoneManagerProtocol.class);
    bucket = mock(OzoneBucket.class);

    when(proxy.getOzoneManagerClient()).thenReturn(omClient);
    when(proxy.getBucketDetails(anyString(), anyString())).thenReturn(bucket);
    when(bucket.getName()).thenReturn("bucket");
    when(bucket.getBucketLayout())
        .thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OzoneClient ozoneClient = mock(OzoneClient.class);
    when(ozoneClient.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getClientProxy()).thenReturn(proxy);

    OzoneClientConfig clientConfig = mock(OzoneClientConfig.class);
    when(clientConfig.getChecksumCombineMode())
        .thenReturn(OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC);

    setField("proxy", proxy);
    setField("objectStore", objectStore);
    setField("ozoneClient", ozoneClient);
    setField("config", new OzoneConfiguration());
    setField("clientConfig", clientConfig);

    // Default to an OM that performs the server-side OBJECT_STORE rejection.
    when(proxy.getOmVersion())
        .thenReturn(OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS);
  }

  private void setField(String name, Object value) throws Exception {
    Field field =
        BasicRootedOzoneClientAdapterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(adapter, value);
  }

  @Test
  public void lookupFileIsTheOnlyOmCall() throws IOException {
    when(omClient.lookupFile(any(OmKeyArgs.class)))
        .thenThrow(new OMException("missing",
            OMException.ResultCodes.FILE_NOT_FOUND));

    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));

    verify(omClient).lookupFile(any(OmKeyArgs.class));
    verify(omClient, never()).lookupKey(any(OmKeyArgs.class));
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
    verify(objectStore, never()).getVolume(anyString());
  }

  @Test
  public void notAFileMappedToFileNotFoundException() throws IOException {
    when(omClient.lookupFile(any(OmKeyArgs.class)))
        .thenThrow(new OMException("Can not write to directory: key",
            OMException.ResultCodes.NOT_A_FILE));

    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));
  }

  @Test
  public void bucketNotFoundMappedToFileNotFoundException() throws IOException {
    when(omClient.lookupFile(any(OmKeyArgs.class)))
        .thenThrow(new OMException("no bucket",
            OMException.ResultCodes.BUCKET_NOT_FOUND));

    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));
  }

  @Test
  public void obsRejectionMappedToIllegalArgumentException()
      throws IOException {
    when(omClient.lookupFile(any(OmKeyArgs.class)))
        .thenThrow(new OMException("Bucket: bucket has layout: OBJECT_STORE",
            OMException.ResultCodes.NOT_SUPPORTED_OPERATION));

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));
    assertThat(ex.getMessage()).contains("OBJECT_STORE");
  }

  @Test
  public void otherOMExceptionPropagates() throws IOException {
    when(omClient.lookupFile(any(OmKeyArgs.class)))
        .thenThrow(new OMException("boom",
            OMException.ResultCodes.INTERNAL_ERROR));

    assertThrows(OMException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));
  }

  @Test
  public void olderOmFallsBackToClientSideBucketCheck() throws IOException {
    // An OM older than LOOKUP_FILE_REJECTS_OBS has no server-side OBJECT_STORE
    // rejection, so the adapter must keep the pre-HDDS-15951 path: fetch the
    // bucket (which validates the layout client-side) and use LookupKey.
    when(proxy.getOmVersion())
        .thenReturn(OzoneManagerVersion.S3_BUCKET_TAGGING_API);
    when(omClient.lookupKey(any(OmKeyArgs.class)))
        .thenThrow(new OMException("missing",
            OMException.ResultCodes.KEY_NOT_FOUND));

    assertThrows(FileNotFoundException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));

    verify(proxy).getBucketDetails(eq("vol"), eq("bucket"));
    verify(omClient).lookupKey(any(OmKeyArgs.class));
    verify(omClient, never()).lookupFile(any(OmKeyArgs.class));
    // The bucket is not a link, so no InfoVolume is needed to resolve layout.
    verify(objectStore, never()).getVolume(anyString());
  }

  @Test
  public void olderOmRejectsObsBucketClientSide() throws IOException {
    when(proxy.getOmVersion())
        .thenReturn(OzoneManagerVersion.S3_BUCKET_TAGGING_API);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> adapter.getFileChecksum(KEY_PATH, 10));
    assertThat(ex.getMessage()).contains("OBJECT_STORE");

    verify(proxy).getBucketDetails(eq("vol"), eq("bucket"));
    verify(omClient, never()).lookupKey(any(OmKeyArgs.class));
    verify(omClient, never()).lookupFile(any(OmKeyArgs.class));
    verify(objectStore, never()).getVolume(anyString());
  }
}
