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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Testing for DeleteBucketLifecycleConfiguration.
 */
public class TestS3LifecycleConfigurationDelete {
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {
    OzoneClient clientStub = new OzoneClientStub();
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
    ObjectStore objectStore = clientStub.getObjectStore();
    objectStore.createS3Bucket("bucket1");
    bucketEndpoint.queryParamsForTest().set(S3Consts.QueryParams.LIFECYCLE, "");
  }

  @Test
  public void testDeleteNonExistentLifecycleConfiguration()
      throws Exception {
    // DeleteBucketLifecycle is idempotent: deleting a non-existent
    // configuration must succeed with 204, not fail with 404.
    Response r = bucketEndpoint.delete("bucket1");
    assertEquals(HTTP_NO_CONTENT, r.getStatus());
  }

  @Test
  public void testDeleteLifecycleConfiguration() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBody());
    Response r = bucketEndpoint.delete(bucketName);

    assertEquals(HTTP_NO_CONTENT, r.getStatus());

    try {
      // Make sure it was deleted.
      bucketEndpoint.get(bucketName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
          ex.getCode());
    }
  }

  private static InputStream getBody() {
    String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Expiration><Days>30</Days></Expiration>" +
        "<Status>Enabled</Status>" +
        "</Rule>" +
        "</LifecycleConfiguration>");

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = "owner")
  public void testDeleteLifecycleLookupCount(String expectedOwner) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, headers);

    try (Response response = endpoint.delete("bucket1")) {
      assertThat(response.getStatus()).isEqualTo(HTTP_NO_CONTENT);
    }

    verify(proxy).getBucketDetails("volume1", "bucket1");
    verify(proxy).deleteLifecycleConfiguration("volume1", "bucket1");
  }

  @Test
  public void testDeleteLifecycleWithoutHeaders() throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, null);
    try (Response response = endpoint.delete("bucket1")) {
      assertThat(response.getStatus()).isEqualTo(HTTP_NO_CONTENT);
    }
    verify(proxy).deleteLifecycleConfiguration("volume1", "bucket1");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = "owner")
  public void testDeleteLifecycleMissingConfigurationIsIdempotent(String expectedOwner) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, headers);
    doThrow(new OMException("Missing configuration", ResultCodes.LIFECYCLE_CONFIGURATION_NOT_FOUND))
        .when(proxy).deleteLifecycleConfiguration("volume1", "bucket1");
    try (Response response = endpoint.delete("bucket1")) {
      assertThat(response.getStatus()).isEqualTo(HTTP_NO_CONTENT);
    }
  }

  @ParameterizedTest
  @CsvSource({", BUCKET_NOT_FOUND, NO_SUCH_BUCKET", "owner, BUCKET_NOT_FOUND, NO_SUCH_BUCKET",
      ", VOLUME_NOT_FOUND, NO_SUCH_BUCKET", "owner, VOLUME_NOT_FOUND, NO_SUCH_BUCKET",
      ", PERMISSION_DENIED, ACCESS_DENIED", "owner, PERMISSION_DENIED, ACCESS_DENIED",
      ", INTERNAL_ERROR, INTERNAL_ERROR", "owner, INTERNAL_ERROR, INTERNAL_ERROR"})
  public void testDeleteLifecycleProtocolErrors(
      String expectedOwner, ResultCodes code, S3ErrorTable expected) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, headers);
    doThrow(new OMException("Delete failed", code)).when(proxy).deleteLifecycleConfiguration("volume1", "bucket1");
    assertErrorResponse(expected, () -> endpoint.delete("bucket1"));
  }

  @Test
  public void testDeleteLifecycleRejectsOwnerBeforeDeleting() throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("other-owner");
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, headers);
    assertErrorResponse(S3ErrorTable.ACCESS_DENIED, () -> endpoint.delete("bucket1"));
    verify(proxy, never()).deleteLifecycleConfiguration("volume1", "bucket1");
  }

  @ParameterizedTest
  @CsvSource({", BUCKET_NOT_FOUND, NO_SUCH_BUCKET", "owner, BUCKET_NOT_FOUND, ACCESS_DENIED",
      ", VOLUME_NOT_FOUND, NO_SUCH_BUCKET", "owner, VOLUME_NOT_FOUND, ACCESS_DENIED",
      ", PERMISSION_DENIED, ACCESS_DENIED", "owner, PERMISSION_DENIED, ACCESS_DENIED",
      ", INTERNAL_ERROR, INTERNAL_ERROR", "owner, INTERNAL_ERROR, ACCESS_DENIED",
      "'', PERMISSION_DENIED, ACCESS_DENIED"})
  public void testDeleteLifecycleLookupFailurePreventsDeletion(
      String expectedOwner, ResultCodes code, S3ErrorTable expected) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    BucketEndpoint endpoint = newProtocolEndpoint(proxy, headers);
    when(proxy.getBucketDetails("volume1", "bucket1")).thenThrow(new OMException("Lookup failed", code));
    assertErrorResponse(expected, () -> endpoint.delete("bucket1"));
    verify(proxy, never()).deleteLifecycleConfiguration("volume1", "bucket1");
  }

  private BucketEndpoint newProtocolEndpoint(ClientProtocol proxy, HttpHeaders requestHeaders) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneVolume volume = OzoneVolume.newBuilder(conf, proxy)
        .setName("volume1").setAcls(Collections.emptyList()).build();
    OzoneBucket bucket = OzoneBucket.newBuilder(conf, proxy)
        .setVolumeName("volume1").setName("bucket1").setOwner("owner").build();
    when(proxy.getBucketDetails("volume1", "bucket1")).thenReturn(bucket);
    ObjectStore store = mock(ObjectStore.class);
    when(store.getS3Volume()).thenReturn(volume);
    when(store.getClientProxy()).thenReturn(proxy);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(store);
    when(client.getProxy()).thenReturn(proxy);
    BucketEndpoint endpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client).setHeaders(requestHeaders).build();
    endpoint.queryParamsForTest().set(S3Consts.QueryParams.LIFECYCLE, "");
    return endpoint;
  }

}
