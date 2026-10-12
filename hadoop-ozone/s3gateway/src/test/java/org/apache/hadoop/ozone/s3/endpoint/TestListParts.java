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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * This class test list parts request.
 */
public class TestListParts {

  private ObjectEndpoint rest;
  private String uploadID;

  @BeforeEach
  public void setUp() throws Exception {

    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();

    uploadID = initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY);

    for (int i = 1; i <= 3; i++) {
      uploadPart(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, i, uploadID, "Multipart Upload");
    }
  }

  @Test
  public void testListParts() throws Exception {
    ListPartsResponse listPartsResponse = listParts(3, 0);

    assertFalse(listPartsResponse.getTruncated());
    assertEquals(3, listPartsResponse.getPartList().size());
  }

  @Test
  public void testListPartsContinuation() throws Exception {
    ListPartsResponse listPartsResponse = listParts(2, 0);

    assertTrue(listPartsResponse.getTruncated());
    assertEquals(2, listPartsResponse.getPartList().size());

    // Continue
    listPartsResponse = listParts(2, listPartsResponse.getNextPartNumberMarker());

    assertFalse(listPartsResponse.getTruncated());
    assertEquals(1, listPartsResponse.getPartList().size());
  }

  @Test
  public void testListPartsWithUnknownUploadID() {
    assertErrorResponse(S3ErrorTable.NO_SUCH_UPLOAD,
        () -> EndpointTestUtils.listParts(rest, OzoneConsts.S3_BUCKET, "no-such-key", "no-such-upload", 2, 0));
  }

  @ParameterizedTest
  @CsvSource({", 0", "'', 0", "owner, 1"})
  public void testListPartsOnlyLooksUpBucketForOwner(String expectedOwner, int bucketLookups) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    ObjectEndpoint endpoint = newEndpoint(proxy, headers);
    OzoneMultipartUploadPartListParts parts = new OzoneMultipartUploadPartListParts(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE), 4, true);
    parts.addPart(new OzoneMultipartUploadPartListParts.PartInfo(4, "part4", 0, 10, "etag4"));
    when(proxy.listParts("volume1", "bucket1", "key1", "upload1", 3, 2)).thenReturn(parts);

    try (Response response = EndpointTestUtils.listParts(endpoint, "bucket1", "key1", "upload1", 2, 3)) {
      assertThat(response.getStatus()).isEqualTo(200);
      ListPartsResponse result = (ListPartsResponse) response.getEntity();
      assertThat(result.getBucket()).isEqualTo("bucket1");
      assertThat(result.getPartNumberMarker()).isEqualTo(3);
      assertThat(result.getMaxParts()).isEqualTo(2);
      assertThat(result.getNextPartNumberMarker()).isEqualTo(4);
      assertThat(result.getTruncated()).isTrue();
      assertThat(result.getPartList()).hasSize(1);
      assertThat(result.getPartList().get(0).getETag()).isEqualTo("etag4");
    }
    verify(proxy, times(bucketLookups)).getBucketDetails("volume1", "bucket1");
    verify(proxy).listParts("volume1", "bucket1", "key1", "upload1", 3, 2);
  }

  @Test
  public void testListPartsRejectsIncorrectOwner() throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("other-owner");
    ObjectEndpoint endpoint = newEndpoint(proxy, headers);

    assertErrorResponse(S3ErrorTable.BUCKET_OWNER_MISMATCH,
        () -> EndpointTestUtils.listParts(endpoint, "bucket1", "key1", "upload1", 2, 3));

    verify(proxy, never()).listParts("volume1", "bucket1", "key1", "upload1", 3, 2);
  }

  @ParameterizedTest
  @CsvSource({
      "NO_SUCH_MULTIPART_UPLOAD_ERROR, NO_SUCH_UPLOAD",
      "BUCKET_NOT_FOUND, NO_SUCH_BUCKET",
      "VOLUME_NOT_FOUND, NO_SUCH_BUCKET",
      "PERMISSION_DENIED, ACCESS_DENIED"
  })
  public void testListPartsProtocolErrors(ResultCodes resultCode, S3ErrorTable expectedError) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    ObjectEndpoint endpoint = newEndpoint(proxy, mock(HttpHeaders.class));
    when(proxy.listParts("volume1", "bucket1", "key1", "upload1", 3, 2))
        .thenThrow(new OMException("List parts failed", resultCode));

    assertErrorResponse(expectedError,
        () -> EndpointTestUtils.listParts(endpoint, "bucket1", "key1", "upload1", 2, 3));

    verify(proxy).listParts("volume1", "bucket1", "key1", "upload1", 3, 2);
  }

  @ParameterizedTest
  @CsvSource({"PERMISSION_DENIED, ACCESS_DENIED", "TOKEN_EXPIRED, EXPIRED_TOKEN"})
  public void testListPartsVolumeLookupErrors(ResultCodes resultCode, S3ErrorTable expectedError) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    ObjectEndpoint endpoint = newEndpoint(proxy, mock(HttpHeaders.class));
    when(endpoint.getClient().getObjectStore().getS3Volume())
        .thenThrow(new OMException("Volume lookup failed", resultCode));

    OS3Exception error = assertErrorResponse(expectedError,
        () -> EndpointTestUtils.listParts(endpoint, "bucket1", "key1", "upload1", 2, 3));

    assertThat(error.getResource()).isEqualTo("key1");
    verify(proxy, never()).listParts("volume1", "bucket1", "key1", "upload1", 3, 2);
  }

  private ObjectEndpoint newEndpoint(ClientProtocol proxy, HttpHeaders headers) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneVolume volume = OzoneVolume.newBuilder(conf, proxy)
        .setName("volume1").setAcls(Collections.emptyList()).build();
    OzoneBucket bucket = OzoneBucket.newBuilder(conf, proxy)
        .setVolumeName("volume1").setName("bucket1").setOwner("owner").build();
    when(proxy.getBucketDetails("volume1", "bucket1")).thenReturn(bucket);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(objectStore.getS3Volume()).thenReturn(volume);
    when(objectStore.getClientProxy()).thenReturn(proxy);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getProxy()).thenReturn(proxy);
    return EndpointBuilder.newObjectEndpointBuilder().setClient(client).setHeaders(headers).build();
  }

  private ListPartsResponse listParts(int maxParts, int nextPart) throws IOException, OS3Exception {
    try (Response response = EndpointTestUtils.listParts(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        uploadID, maxParts, nextPart)) {
      return (ListPartsResponse) response.getEntity();
    }
  }
}
