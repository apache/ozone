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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.abortMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertStatus;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * This class tests abort multipart upload request.
 */
public class TestAbortMultipartUpload {

  @BeforeEach
  public void setup() {
    S3GatewayMetrics.create(new OzoneConfiguration());
  }

  @Test
  public void testAbortMultipartUpload() throws Exception {

    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = spy(new OzoneClientStub());
    ObjectStore store = spy(client.getObjectStore());
    doReturn(store).when(client).getObjectStore();
    client.getObjectStore().createS3Bucket(bucket);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();

    String uploadID = initiateMultipartUpload(rest, bucket, key);

    clearInvocations(store);

    // Abort multipart upload
    assertStatus(HttpStatus.SC_NO_CONTENT, () -> abortMultipartUpload(rest, bucket, key, uploadID));

    verify(store, never()).getS3Volume();

    // test with unknown upload Id.
    assertErrorResponse(S3ErrorTable.NO_SUCH_UPLOAD, () -> abortMultipartUpload(rest, bucket, key, "random"));
  }

  @ParameterizedTest
  @CsvSource({", 0", "'', 0", "owner, 1"})
  public void testAbortOnlyLooksUpBucketForOwner(String expectedOwner, int bucketLookups) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    ObjectEndpoint endpoint = newEndpoint(proxy, headers);
    String volumeName = bucketLookups == 0 ? null : "volume1";

    assertStatus(HttpStatus.SC_NO_CONTENT, () -> abortMultipartUpload(endpoint, "bucket1", "key1", "upload1"));

    verify(endpoint.getClient().getObjectStore(), never()).getS3Volume();
    verify(proxy, times(bucketLookups)).getBucketDetails(null, "bucket1");
    verify(proxy).abortMultipartUpload(volumeName, "bucket1", "key1", "upload1");
  }

  @Test
  public void testAbortMapsPermissionDenied() throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    ObjectEndpoint endpoint = newEndpoint(proxy, mock(HttpHeaders.class));
    doThrow(new OMException("Abort failed", ResultCodes.PERMISSION_DENIED)).when(proxy)
        .abortMultipartUpload(null, "bucket1", "key1", "upload1");

    assertErrorResponse(S3ErrorTable.ACCESS_DENIED, () -> abortMultipartUpload(endpoint, "bucket1", "key1", "upload1"));

    verify(proxy).abortMultipartUpload(null, "bucket1", "key1", "upload1");
  }

  private ObjectEndpoint newEndpoint(ClientProtocol proxy, HttpHeaders headers) throws Exception {
    OzoneBucket bucket = OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName("volume1").setName("bucket1").setOwner("owner").build();
    when(proxy.getBucketDetails(null, "bucket1")).thenReturn(bucket);
    ObjectStore store = mock(ObjectStore.class);
    when(store.getClientProxy()).thenReturn(proxy);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(store);
    when(client.getProxy()).thenReturn(proxy);
    return EndpointBuilder.newObjectEndpointBuilder().setClient(client).setHeaders(headers).build();
  }

}
