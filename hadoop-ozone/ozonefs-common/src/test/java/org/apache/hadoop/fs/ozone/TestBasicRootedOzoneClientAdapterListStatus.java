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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.protocol.ListStatusLightOptions;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListStatusResult;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

/**
 * Unit tests for {@link BasicRootedOzoneClientAdapterImpl#listStatus} on a key
 * path, covering the listing without InfoBucket added by HDDS-16760, the
 * orphan link bucket handling on the error path, and the fallback used when
 * the OM predates {@link OzoneManagerVersion#LIST_STATUS_REJECTS_OBS}. Uses a
 * partial mock so no OM connection is required.
 */
public class TestBasicRootedOzoneClientAdapterListStatus {

  private static final String DIR_PATH = "/vol/bucket/dir";
  private static final URI OFS_URI = URI.create("ofs://om/");

  private BasicRootedOzoneClientAdapterImpl adapter;
  private ClientProtocol proxy;
  private ObjectStore objectStore;
  private OzoneBucket bucket;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    // Real default methods, so the page methods delegate to the stubbed listStatus / listStatusLight.
    proxy = mock(ClientProtocol.class, CALLS_REAL_METHODS);
    objectStore = mock(ObjectStore.class);
    bucket = mock(OzoneBucket.class);

    when(proxy.getBucketDetails(anyString(), anyString())).thenReturn(bucket);
    when(bucket.getVolumeName()).thenReturn("vol");
    when(bucket.getName()).thenReturn("bucket");
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    // Mirror OzoneBucket: the source path exists unless link resolution clears it.
    AtomicBoolean sourcePathExist = new AtomicBoolean(true);
    doAnswer(invocation -> {
      sourcePathExist.set(invocation.getArgument(0));
      return null;
    }).when(bucket).setSourcePathExist(anyBoolean());
    when(bucket.isSourcePathExist()).thenAnswer(invocation -> sourcePathExist.get());

    setField("proxy", proxy);
    setField("objectStore", objectStore);
    setField("config", new OzoneConfiguration());

    // Default to an OM that performs the server-side OBJECT_STORE rejection.
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.LIST_STATUS_REJECTS_OBS);
  }

  private void setField(String name, Object value) throws Exception {
    Field field = BasicRootedOzoneClientAdapterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(adapter, value);
  }

  private List<FileStatusAdapter> listStatus(boolean lite) throws IOException {
    return adapter.listStatus(DIR_PATH, false, "", 100, OFS_URI, new Path("/"), "user", lite);
  }

  private ListStatusResult<FileStatusAdapter> listStatusPage(String path, boolean lite) throws IOException {
    return adapter.listStatusPage(path, false, "", 100, OFS_URI, new Path("/"), "user", lite);
  }

  private void failListing(OMException e) throws IOException {
    when(proxy.listStatus(any(ListStatusLightOptions.class))).thenThrow(e);
    when(proxy.listStatusLight(any(ListStatusLightOptions.class))).thenThrow(e);
  }

  /** The options the adapter sends: the listed path, asking OM to reject OBJECT_STORE. */
  private static ListStatusLightOptions fsListing() {
    return argThat(o -> o.getVolumeName().equals("vol") && o.getBucketName().equals("bucket")
        && o.getKeyName().equals("dir") && !o.isRecursive() && o.getNumEntries() == 100
        && o.isRequireFileSystemLayout());
  }

  /** Makes the bucket a link whose source volume has been deleted. */
  private void makeOrphanLink() throws IOException {
    when(bucket.isLink()).thenReturn(true);
    when(bucket.getSourceVolume()).thenReturn("srcvol");
    when(bucket.getSourceBucket()).thenReturn("srcbucket");
    when(objectStore.getVolume("srcvol"))
        .thenThrow(new OMException("no volume", ResultCodes.VOLUME_NOT_FOUND));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void listingIsTheOnlyOmCall(boolean lite) throws IOException {
    assertThat(listStatus(lite)).isEmpty();

    if (lite) {
      verify(proxy).listStatusLight(fsListing());
    } else {
      verify(proxy).listStatus(fsListing());
    }
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
    verify(objectStore, never()).getVolume(anyString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void listingPassesOnWhetherOmReportedTheListingComplete(boolean lite) throws IOException {
    for (boolean truncated : new boolean[] {true, false}) {
      when(proxy.listStatusLightPage(any(ListStatusLightOptions.class)))
          .thenReturn(ListStatusResult.<OzoneFileStatusLight>of(Collections.emptyList(), truncated));
      when(proxy.listStatusPage(any(ListStatusLightOptions.class)))
          .thenReturn(ListStatusResult.<OzoneFileStatus>of(Collections.emptyList(), truncated));

      assertEquals(!truncated, listStatusPage(DIR_PATH, lite).isComplete());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void listingWithoutOmReportIsNotComplete(boolean lite) throws IOException {
    // The default page methods wrap listStatus / listStatusLight, as for an OM that does not set the flag.
    assertFalse(listStatusPage(DIR_PATH, lite).isComplete());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "/vol"})
  public void rootAndVolumeListingsAreNotComplete(String path) throws IOException {
    // Root and volume listings do not come from ListStatus, so they cannot report completeness.
    when(objectStore.listVolumesByUser(anyString(), any(), any())).thenAnswer(i -> Collections.emptyIterator());
    OzoneVolume volume = mock(OzoneVolume.class);
    when(objectStore.getVolume("vol")).thenReturn(volume);
    when(volume.listBuckets(any(), any())).thenAnswer(i -> Collections.emptyIterator());

    assertFalse(listStatusPage(path, true).isComplete());
  }

  @ParameterizedTest
  @EnumSource(value = ResultCodes.class, names = {"BUCKET_NOT_FOUND", "VOLUME_NOT_FOUND"})
  public void orphanLinkReturnsEmptyListAfterFailedListing(ResultCodes code) throws IOException {
    // OM cannot resolve the link to its deleted source, so the listing fails
    // and only then does the client fetch the bucket to recognise the orphan.
    failListing(new OMException("source missing", code));
    makeOrphanLink();

    for (boolean lite : new boolean[] {true, false}) {
      assertThat(listStatus(lite)).isEmpty();
    }

    InOrder order = inOrder(proxy);
    order.verify(proxy).listStatusLight(fsListing());
    order.verify(proxy).getBucketDetails("vol", "bucket");
    order.verify(proxy).listStatus(fsListing());
    order.verify(proxy).getBucketDetails("vol", "bucket");
  }

  @Test
  public void orphanLinkDeclaredObjectStoreIsRejected() throws IOException {
    // The fallback validates the link's own layout for an orphan; the error
    // path must keep doing so.
    failListing(new OMException("source missing", ResultCodes.BUCKET_NOT_FOUND));
    makeOrphanLink();
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> listStatus(false));
    assertThat(ex.getMessage()).contains("OBJECT_STORE");
  }

  @Test
  public void missingBucketPropagatesFromBucketFetch() throws IOException {
    failListing(new OMException("no bucket in listing", ResultCodes.BUCKET_NOT_FOUND));
    OMException missing = new OMException("no bucket", ResultCodes.BUCKET_NOT_FOUND);
    when(proxy.getBucketDetails("vol", "bucket")).thenThrow(missing);

    OMException ex = assertThrows(OMException.class, () -> listStatus(false));
    assertSame(missing, ex);
  }

  @Test
  public void existingBucketOnErrorPathRethrowsListingFailure() throws IOException {
    // The bucket exists by the time it is fetched, so it is not an orphan link.
    OMException listingFailure = new OMException("no bucket", ResultCodes.BUCKET_NOT_FOUND);
    failListing(listingFailure);

    OMException ex = assertThrows(OMException.class, () -> listStatus(false));
    assertSame(listingFailure, ex);
  }

  @Test
  public void obsRejectionMappedToIllegalArgumentException() throws IOException {
    failListing(new OMException("Bucket: bucket has layout: OBJECT_STORE", ResultCodes.NOT_SUPPORTED_OPERATION));

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> listStatus(false));
    assertThat(ex.getMessage()).contains("OBJECT_STORE");
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @Test
  public void fileNotFoundMappedToFileNotFoundException() throws IOException {
    failListing(new OMException("no dir", ResultCodes.FILE_NOT_FOUND));

    assertThrows(FileNotFoundException.class, () -> listStatus(false));
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @Test
  public void otherOMExceptionPropagates() throws IOException {
    failListing(new OMException("denied", ResultCodes.PERMISSION_DENIED));

    OMException ex = assertThrows(OMException.class, () -> listStatus(false));
    assertEquals(ResultCodes.PERMISSION_DENIED, ex.getResult());
    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  @Test
  public void olderOmFallsBackToClientSideBucketCheck() throws IOException {
    // An OM older than LIST_STATUS_REJECTS_OBS has no server-side OBJECT_STORE
    // rejection, so the adapter must fetch the bucket and validate its layout.
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS);

    assertThat(listStatus(false)).isEmpty();

    verify(proxy).getBucketDetails("vol", "bucket");
    verify(bucket).listStatus("dir", false, "", 100);
    verify(proxy, never()).listStatus(any(ListStatusLightOptions.class));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void zduOmFallsBackToClientSideBucketCheck(boolean lite) throws IOException {
    // An OM finalized for ZDU reports ZDU even without this change, and such an
    // OM ignores requireFileSystemLayout, so the client must still fetch the
    // bucket and validate its layout.
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.ZDU);

    assertThat(listStatus(lite)).isEmpty();

    verify(proxy).getBucketDetails("vol", "bucket");
    if (lite) {
      verify(bucket).listStatusLight("dir", false, "", 100);
    } else {
      verify(bucket).listStatus("dir", false, "", 100);
    }
    verify(proxy, never()).listStatus(any(ListStatusLightOptions.class));
    verify(proxy, never()).listStatusLight(any(ListStatusLightOptions.class));
  }

  @Test
  public void olderOmRejectsObsBucketClientSide() throws IOException {
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS);
    when(bucket.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> listStatus(false));
    assertThat(ex.getMessage()).contains("OBJECT_STORE");
    verify(bucket, never()).listStatus(anyString(), anyBoolean(), anyString(), anyLong());
  }

  @Test
  public void olderOmOrphanLinkReturnsEmptyList() throws IOException {
    when(proxy.getOmVersion()).thenReturn(OzoneManagerVersion.LOOKUP_FILE_REJECTS_OBS);
    makeOrphanLink();

    assertThat(listStatus(false)).isEmpty();
    verify(bucket, never()).listStatus(anyString(), anyBoolean(), anyString(), anyLong());
  }
}
