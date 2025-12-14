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

import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for AclHandler.
 */
public class TestAclHandler {

  private static final String BUCKET_NAME = OzoneConsts.S3_BUCKET;
  private OzoneClient client;
  private BucketEndpointContext context;
  private AclHandler aclHandler;
  private HttpHeaders headers;

  @BeforeEach
  public void setup() throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    headers = mock(HttpHeaders.class);

    BucketEndpoint bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    context = new BucketEndpointContext(bucketEndpoint);
    aclHandler = new AclHandler();
  }

  @AfterEach
  public void clean() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testGetQueryParamName() {
    assertEquals("acl", aclHandler.getQueryParamName(),
        "Query param name should be 'acl'");
  }

  @Test
  public void testHandlePutRequestWithReadHeader() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithWriteHeader() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithReadAcpHeader() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ_ACP))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithWriteAcpHeader() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_WRITE_ACP))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithFullControlHeader() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithMultipleHeaders() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser1\"");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn("id=\"testuser2\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL with multiple headers should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithUnsupportedGranteeType() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("uri=\"http://example.com\"");

    long startNanos = System.nanoTime();
    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest(BUCKET_NAME, null, headers, context,
          startNanos);
    }, "Should throw OS3Exception for unsupported grantee type");

    assertEquals(HTTP_NOT_IMPLEMENTED, exception.getHttpCode(),
        "Should return NOT_IMPLEMENTED for unsupported grantee type");
  }

  @Test
  public void testHandlePutRequestWithEmailAddressType() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("emailAddress=\"test@example.com\"");

    long startNanos = System.nanoTime();
    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest(BUCKET_NAME, null, headers, context,
          startNanos);
    }, "Should throw OS3Exception for email address grantee type");

    assertEquals(HTTP_NOT_IMPLEMENTED, exception.getHttpCode(),
        "Should return NOT_IMPLEMENTED for email address grantee type");
  }

  @Test
  public void testHandlePutRequestBucketNotFound() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();
    assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest("nonexistent-bucket", null, headers,
          context, startNanos);
    }, "Should throw OS3Exception for non-existent bucket");
  }

  @Test
  public void testHandlePutRequestWithBody() throws Exception {
    String aclXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
        "  <Owner>\n" +
        "    <ID>testowner</ID>\n" +
        "    <DisplayName>Test Owner</DisplayName>\n" +
        "  </Owner>\n" +
        "  <AccessControlList>\n" +
        "    <Grant>\n" +
        "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
        "xsi:type=\"CanonicalUser\">\n" +
        "        <ID>testuser</ID>\n" +
        "      </Grantee>\n" +
        "      <Permission>READ</Permission>\n" +
        "    </Grant>\n" +
        "  </AccessControlList>\n" +
        "</AccessControlPolicy>";

    InputStream body = new ByteArrayInputStream(
        aclXml.getBytes(StandardCharsets.UTF_8));

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, body, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL with body should return 200 OK");
  }

  @Test
  public void testHandlePutRequestWithInvalidHeaderFormat() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("invalid-format");

    long startNanos = System.nanoTime();
    assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest(BUCKET_NAME, null, headers, context,
          startNanos);
    }, "Should throw OS3Exception for invalid header format");
  }

  @Test
  public void testHandlePutRequestWithMultipleGrantees() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"user1\",id=\"user2\"");

    long startNanos = System.nanoTime();
    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL with multiple grantees should return 200 OK");
  }

  @Test
  public void testPutAclReplacesExistingAcls() throws Exception {
    // Set initial ACL
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"user1\"");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn(null);

    long startNanos = System.nanoTime();
    aclHandler.handlePutRequest(BUCKET_NAME, null, headers, context,
        startNanos);

    // Replace with new ACL
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(null);
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn("id=\"user2\"");

    Response response = aclHandler.handlePutRequest(
        BUCKET_NAME, null, headers, context, startNanos);

    assertEquals(HTTP_OK, response.getStatus(),
        "PUT ACL should replace existing ACLs");
  }

  @Test
  public void testAuditLoggingOnBucketNotFound() throws Exception {
    // Create a spy of BucketEndpoint to verify audit logging
    BucketEndpoint spyEndpoint = spy(EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build());

    BucketEndpointContext spyContext = new BucketEndpointContext(spyEndpoint);

    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    long startNanos = System.nanoTime();

    // This should throw exception for non-existent bucket
    assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest("nonexistent-bucket", null, headers,
          spyContext, startNanos);
    });

    // Verify that auditWriteFailure was called with PUT_ACL action
    // Note: getBucket() wraps OMException as OS3Exception, so we catch OS3Exception
    verify(spyEndpoint, times(1)).auditWriteFailure(
        eq(S3GAction.PUT_ACL),
        any(OS3Exception.class));
  }

  @Test
  public void testAuditLoggingOnInvalidArgument() throws Exception {
    // Create a spy of BucketEndpoint to verify audit logging
    BucketEndpoint spyEndpoint = spy(EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build());

    BucketEndpointContext spyContext = new BucketEndpointContext(spyEndpoint);

    // Invalid format will trigger OS3Exception
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("invalid-format");

    long startNanos = System.nanoTime();

    assertThrows(OS3Exception.class, () -> {
      aclHandler.handlePutRequest(BUCKET_NAME, null, headers,
          spyContext, startNanos);
    });

    // Verify that auditWriteFailure was called with PUT_ACL action
    verify(spyEndpoint, times(1)).auditWriteFailure(
        eq(S3GAction.PUT_ACL),
        any(OS3Exception.class));
  }
}
