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
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration;
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

/**
 * Testing for GetBucketLifecycleConfiguration.
 */
public class TestS3LifecycleConfigurationGet {
  
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
  public void testGetNonExistentLifecycleConfiguration()
      throws Exception {
    try {
      bucketEndpoint.get("bucket1");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
          ex.getCode());
    }
  }

  @Test
  public void testGetLifecycleConfiguration() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBody());
    Response r = bucketEndpoint.get(bucketName);

    assertEquals(HTTP_OK, r.getStatus());
    S3LifecycleConfiguration lcc =
        (S3LifecycleConfiguration) r.getEntity();
    assertEquals("remove logs after 30 days",
        lcc.getRules().get(0).getId());
    assertEquals("prefix/", lcc.getRules().get(0).getPrefix());
    assertEquals("Enabled", lcc.getRules().get(0).getStatus());
    assertEquals(30,
        lcc.getRules().get(0).getExpiration().getDays().intValue());
  }

  @Test
  public void testGetLifecycleWithAbortIncompleteMultipartUpload() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBodyWithAbortAction());
    Response r = bucketEndpoint.get(bucketName);

    assertEquals(HTTP_OK, r.getStatus());
    S3LifecycleConfiguration lcc = (S3LifecycleConfiguration) r.getEntity();
    assertEquals(1, lcc.getRules().size());
    S3LifecycleConfiguration.Rule rule = lcc.getRules().get(0);

    assertEquals("abort-incomplete-uploads", rule.getId());
    assertEquals("uploads/", rule.getPrefix());
    assertEquals("Enabled", rule.getStatus());
    assertEquals(7, rule.getAbortIncompleteMultipartUpload()
        .getDaysAfterInitiation().intValue());
  }

  @Test
  public void testGetLifecycleWithBothActions() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBodyWithBothActions());
    Response r = bucketEndpoint.get(bucketName);

    assertEquals(HTTP_OK, r.getStatus());
    S3LifecycleConfiguration lcc = (S3LifecycleConfiguration) r.getEntity();
    assertEquals(1, lcc.getRules().size());
    S3LifecycleConfiguration.Rule rule = lcc.getRules().get(0);

    assertEquals("cleanup-rule", rule.getId());
    assertEquals("temp/", rule.getPrefix());
    assertEquals("Enabled", rule.getStatus());

    // Verify Expiration action
    assertEquals(30, rule.getExpiration().getDays().intValue());

    // Verify AbortIncompleteMultipartUpload action
    assertEquals(7, rule.getAbortIncompleteMultipartUpload()
        .getDaysAfterInitiation().intValue());
  }

  @Test
  public void testGetLifecycleWithDateBasedExpiration() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBodyWithDateExpiration());
    Response r = bucketEndpoint.get(bucketName);

    assertEquals(HTTP_OK, r.getStatus());
    S3LifecycleConfiguration lcc = (S3LifecycleConfiguration) r.getEntity();
    assertEquals(1, lcc.getRules().size());
    S3LifecycleConfiguration.Rule rule = lcc.getRules().get(0);

    assertEquals("expire-on-date", rule.getId());
    assertEquals("prefix/", rule.getPrefix());
    assertEquals("Enabled", rule.getStatus());
    assertEquals("2044-01-19T00:00:00+00:00", rule.getExpiration().getDate());
    assertNull(rule.getExpiration().getDays());
  }

  @ParameterizedTest
  @CsvSource({", 0", "'', 0", "owner, 1"})
  public void testGetLifecycleOnlyLooksUpBucketForOwner(String expectedOwner, int bucketLookups) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    BucketEndpoint endpoint = newEndpoint(proxy, headers);
    when(proxy.getLifecycleConfiguration("volume1", "bucket1"))
        .thenReturn(new OzoneLifecycleConfiguration("volume1", "bucket1", 0, Collections.emptyList()));

    try (Response response = endpoint.get("bucket1")) {
      assertThat(response.getStatus()).isEqualTo(HTTP_OK);
      assertThat(((S3LifecycleConfiguration) response.getEntity()).getRules()).isEmpty();
    }

    verify(proxy, times(bucketLookups)).getBucketDetails("volume1", "bucket1");
    verify(proxy).getLifecycleConfiguration("volume1", "bucket1");
  }

  @Test
  public void testGetLifecycleRejectsIncorrectOwnerBeforeReadingConfiguration() throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("other-owner");
    BucketEndpoint endpoint = newEndpoint(proxy, headers);

    assertErrorResponse(ACCESS_DENIED, () -> endpoint.get("bucket1"));

    verify(proxy).getBucketDetails("volume1", "bucket1");
    verify(proxy, never()).getLifecycleConfiguration("volume1", "bucket1");
  }

  @ParameterizedTest
  @CsvSource({
      "LIFECYCLE_CONFIGURATION_NOT_FOUND, NO_SUCH_LIFECYCLE_CONFIGURATION",
      "BUCKET_NOT_FOUND, NO_SUCH_BUCKET",
      "VOLUME_NOT_FOUND, NO_SUCH_BUCKET",
      "PERMISSION_DENIED, ACCESS_DENIED"
  })
  public void testGetLifecycleProtocolErrors(ResultCodes resultCode, S3ErrorTable expectedError) throws Exception {
    ClientProtocol proxy = mock(ClientProtocol.class);
    BucketEndpoint endpoint = newEndpoint(proxy, mock(HttpHeaders.class));
    when(proxy.getLifecycleConfiguration("volume1", "bucket1"))
        .thenThrow(new OMException("Lifecycle lookup failed", resultCode));

    assertErrorResponse(expectedError, () -> endpoint.get("bucket1"));

    verify(proxy, never()).getBucketDetails("volume1", "bucket1");
    verify(proxy).getLifecycleConfiguration("volume1", "bucket1");
  }

  private BucketEndpoint newEndpoint(ClientProtocol proxy, HttpHeaders headers) throws Exception {
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
    BucketEndpoint endpoint = EndpointBuilder.newBucketEndpointBuilder().setClient(client).setHeaders(headers).build();
    endpoint.queryParamsForTest().set(S3Consts.QueryParams.LIFECYCLE, "");
    return endpoint;
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

  private static InputStream getBodyWithAbortAction() {
    String xml = "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>abort-incomplete-uploads</ID>" +
        "<Prefix>uploads/</Prefix>" +
        "<Status>Enabled</Status>" +
        "<AbortIncompleteMultipartUpload>" +
        "<DaysAfterInitiation>7</DaysAfterInitiation>" +
        "</AbortIncompleteMultipartUpload>" +
        "</Rule>" +
        "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream getBodyWithDateExpiration() {
    String xml = "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>expire-on-date</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Status>Enabled</Status>" +
        "<Expiration><Date>2044-01-19T00:00:00+00:00</Date></Expiration>" +
        "</Rule>" +
        "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream getBodyWithBothActions() {
    String xml = "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>cleanup-rule</ID>" +
        "<Prefix>temp/</Prefix>" +
        "<Status>Enabled</Status>" +
        "<Expiration>" +
        "<Days>30</Days>" +
        "</Expiration>" +
        "<AbortIncompleteMultipartUpload>" +
        "<DaysAfterInitiation>7</DaysAfterInitiation>" +
        "</AbortIncompleteMultipartUpload>" +
        "</Rule>" +
        "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }
}
