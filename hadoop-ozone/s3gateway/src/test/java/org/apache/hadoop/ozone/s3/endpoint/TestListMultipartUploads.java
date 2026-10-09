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
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneMultipartUpload;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests listing multipart uploads through the bucket endpoint. */
public class TestListMultipartUploads {
  private ClientProtocol proxy;
  private HttpHeaders headers;
  private BucketEndpoint endpoint;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    S3GatewayMetrics.create(conf);
    proxy = mock(ClientProtocol.class);
    headers = mock(HttpHeaders.class);
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
    endpoint = EndpointBuilder.newBucketEndpointBuilder().setClient(client).setHeaders(headers).build();
    endpoint.queryParamsForTest().set(QueryParams.UPLOADS, "");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "prefix/");
    endpoint.queryParamsForTest().set(QueryParams.KEY_MARKER, "prefix/previous");
    endpoint.queryParamsForTest().set(QueryParams.UPLOAD_ID_MARKER, "previous-upload");
    endpoint.queryParamsForTest().set(QueryParams.MAX_UPLOADS, "2");
  }

  @ParameterizedTest
  @CsvSource({", 0", "'', 0", "owner, 1"})
  public void testOnlyLooksUpBucketForOwner(String expectedOwner, int bucketLookups) throws Exception {
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    OzoneMultipartUpload upload = new OzoneMultipartUpload("volume1", "bucket1", "prefix/key", "upload1",
        Instant.EPOCH, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));
    when(proxy.listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload", 2))
        .thenReturn(new OzoneMultipartUploadList(Collections.singletonList(upload), "prefix/key", "upload1", true));

    try (Response response = endpoint.get("bucket1")) {
      assertThat(response.getStatus()).isEqualTo(200);
      ListMultipartUploadsResult result = (ListMultipartUploadsResult) response.getEntity();
      assertThat(result.getBucket()).isEqualTo("bucket1");
      assertThat(result.getPrefix()).isEqualTo("prefix/");
      assertThat(result.getKeyMarker()).isEqualTo("prefix/previous");
      assertThat(result.getUploadIdMarker()).isEqualTo("previous-upload");
      assertThat(result.getNextKeyMarker()).isEqualTo("prefix/key");
      assertThat(result.getNextUploadIdMarker()).isEqualTo("upload1");
      assertThat(result.getMaxUploads()).isEqualTo(2);
      assertThat(result.isTruncated()).isTrue();
      assertThat(result.getUploads()).hasSize(1);
      assertThat(result.getUploads().get(0).getKey()).isEqualTo("prefix/key");
    }
    verify(proxy, times(bucketLookups)).getBucketDetails("volume1", "bucket1");
    verify(proxy).listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload", 2);
  }

  @Test
  public void testRejectsIncorrectOwner() throws Exception {
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("other-owner");

    assertErrorResponse(S3ErrorTable.BUCKET_OWNER_MISMATCH, () -> endpoint.get("bucket1"));

    verify(proxy).getBucketDetails("volume1", "bucket1");
    verify(proxy, never()).listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous",
        "previous-upload", 2);
  }

  @ParameterizedTest
  @CsvSource({
      "BUCKET_NOT_FOUND, NO_SUCH_BUCKET", "VOLUME_NOT_FOUND, NO_SUCH_BUCKET", "PERMISSION_DENIED, ACCESS_DENIED"
  })
  public void testProtocolErrors(ResultCodes resultCode, S3ErrorTable expectedError) throws Exception {
    when(proxy.listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload", 2))
        .thenThrow(new OMException("List uploads failed", resultCode));

    assertErrorResponse(expectedError, () -> endpoint.get("bucket1"));

    verify(proxy, never()).getBucketDetails("volume1", "bucket1");
    verify(proxy).listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload", 2);
  }

  @ParameterizedTest
  @CsvSource({"1, 1", "1000, 1000", "1001, 1000"})
  public void testMaxUploadsAndEmptyResult(int requested, int forwarded) throws Exception {
    endpoint.queryParamsForTest().set(QueryParams.MAX_UPLOADS, Integer.toString(requested));
    when(proxy.listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload", forwarded))
        .thenReturn(new OzoneMultipartUploadList(Collections.emptyList(), null, null, false));

    try (Response response = endpoint.get("bucket1")) {
      ListMultipartUploadsResult result = (ListMultipartUploadsResult) response.getEntity();
      assertThat(result.getMaxUploads()).isEqualTo(forwarded);
      assertThat(result.getUploads()).isEmpty();
      assertThat(result.isTruncated()).isFalse();
    }
    verify(proxy, never()).getBucketDetails("volume1", "bucket1");
    verify(proxy).listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous",
        "previous-upload", forwarded);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  public void testRejectsInvalidMaxUploads(int maxUploads) throws Exception {
    endpoint.queryParamsForTest().set(QueryParams.MAX_UPLOADS, Integer.toString(maxUploads));

    assertErrorResponse(S3ErrorTable.INVALID_ARGUMENT, () -> endpoint.get("bucket1"));

    verify(proxy, never()).getBucketDetails("volume1", "bucket1");
    verify(proxy, never()).listMultipartUploads("volume1", "bucket1", "prefix/", "prefix/previous", "previous-upload",
        maxUploads);
  }
}
