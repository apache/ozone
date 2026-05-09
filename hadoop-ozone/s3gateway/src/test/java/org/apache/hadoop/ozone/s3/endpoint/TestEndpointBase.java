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

import static org.apache.hadoop.ozone.s3.util.S3Consts.CACHED_BUCKETS_CONTEXT_PROPERTY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ORIGIN_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo.Version;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests EndpointBase behaviors.
 */
public class TestEndpointBase {

  @Test
  public void testFilterGDPRFromCustomMetadataHeaders() throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + OzoneConsts.GDPR_FLAG, "true");

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> filteredCustomMetadata =
        endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);
    assertThat(filteredCustomMetadata).containsKey("custom-key1");
    assertEquals("custom-value1", filteredCustomMetadata.get("custom-key1"));
    assertThat(filteredCustomMetadata).containsKey("custom-key2");
    assertEquals("custom-value2", filteredCustomMetadata.get("custom-key2"));
    assertThat(filteredCustomMetadata).doesNotContainKey(OzoneConsts.GDPR_FLAG);
  }

  @Test
  public void testCustomMetadataHeadersSizeOverbig() {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + "custom-key1", "custom-value1");
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + "custom-key2", "custom-value2");
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX + "custom-key3",
        new String(new byte[3000], StandardCharsets.UTF_8));

    EndpointBase endpointBase = new EndpointBase() {
    };

    OS3Exception e = assertThrows(OS3Exception.class,
        () -> endpointBase.getCustomMetadataFromHeaders(s3requestHeaders));
    assertThat(e.getCode()).contains("MetadataTooLarge");
  }

  @Test
  public void testCustomMetadataHeadersWithUpperCaseHeaders()
      throws OS3Exception {
    MultivaluedMap<String, String> s3requestHeaders = new MultivaluedHashMap<>();
    String key = "CUSTOM-KEY";
    String value = "custom-value1";
    s3requestHeaders.add(
        CUSTOM_METADATA_HEADER_PREFIX.toUpperCase(Locale.ROOT) + key, value);

    EndpointBase endpointBase = new EndpointBase() {
    };

    Map<String, String> customMetadata =
        endpointBase.getCustomMetadataFromHeaders(s3requestHeaders);

    assertEquals(value, customMetadata.get(key));
  }

  @Test
  public void clearsThreadLocalS3AuthForUnsignedPreflight() {
    ClientProtocol proxy = mock(ClientProtocol.class);

    EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient(proxy))
        .setSignatureInfo(new SignatureInfo.Builder(Version.NONE).build())
        .build();

    verify(proxy).setIsS3Request(true);
    verify(proxy).clearThreadLocalS3Auth();
    verify(proxy, never()).setThreadLocalS3Auth(any());
  }

  @Test
  public void setsThreadLocalS3AuthForSignedRequests() {
    ClientProtocol proxy = mock(ClientProtocol.class);
    SignatureInfo signatureInfo = new SignatureInfo.Builder(Version.V4)
        .setAwsAccessId("testuser")
        .setSignature("signature")
        .setStringToSign("string-to-sign")
        .build();

    EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient(proxy))
        .setSignatureInfo(signatureInfo)
        .build();

    ArgumentCaptor<S3Auth> authCaptor = ArgumentCaptor.forClass(S3Auth.class);
    verify(proxy).setIsS3Request(true);
    verify(proxy).setThreadLocalS3Auth(authCaptor.capture());
    verify(proxy, never()).clearThreadLocalS3Auth();
    assertEquals("testuser", authCaptor.getValue().getAccessID());
    assertEquals("testuser", authCaptor.getValue().getUserPrincipal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void cacheBucketPublishesBucketForCorsRequests() {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
    when(requestContext.getHeaderString(ORIGIN_HEADER))
        .thenReturn("https://example.com");
    when(headers.getHeaderString(ORIGIN_HEADER))
        .thenReturn("https://example.com");

    EndpointBase endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient(proxy))
        .setContext(requestContext)
        .setHeaders(headers)
        .build();

    endpoint.cacheBucket("bucket", bucket);

    ArgumentCaptor<Map<String, OzoneBucket>> bucketsCaptor =
        ArgumentCaptor.forClass(Map.class);
    verify(requestContext).setProperty(eq(CACHED_BUCKETS_CONTEXT_PROPERTY),
        bucketsCaptor.capture());
    assertThat(bucketsCaptor.getValue()).containsEntry("bucket", bucket);
  }

  @Test
  public void cacheBucketSkipsPublishingBucketWithoutOriginHeader() {
    ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket bucket = mock(OzoneBucket.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());

    EndpointBase endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient(proxy))
        .setContext(requestContext)
        .setHeaders(headers)
        .build();

    endpoint.cacheBucket("bucket", bucket);

    verify(requestContext, never()).setProperty(
        eq(CACHED_BUCKETS_CONTEXT_PROPERTY), any());
  }

  private static OzoneClient mockClient(ClientProtocol proxy) {
    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(objectStore.getClientProxy()).thenReturn(proxy);
    return client;
  }
}
