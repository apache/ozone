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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.stream.Stream;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for BucketAclHandler.
 */
public class TestBucketAclHandler {

  private static final String BUCKET_NAME = OzoneConsts.S3_BUCKET;
  private OzoneClient client;
  private BucketAclHandler aclHandler;
  private HttpHeaders headers;

  @BeforeEach
  public void setup() throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    headers = mock(HttpHeaders.class);

    // Build BucketAclHandler using EndpointBuilder since it extends EndpointBase
    aclHandler = EndpointBuilder.newBucketAclHandlerBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    // Set up query parameter for ACL operation (default for most tests)
    aclHandler.queryParamsForTest().set("acl", "");
  }

  @AfterEach
  public void clean() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testHandlePutRequestWithAclQueryParam() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    assertNotNull(aclHandler.handlePutRequest(BUCKET_NAME, null),
        "Handler should handle request with ?acl param");
  }

  @Test
  public void testHandlePutRequestWithoutAclQueryParam() throws Exception {
    // Remove "acl" query parameter - handler should not handle request
    aclHandler.queryParamsForTest().unset("acl");
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    Response response = aclHandler.handlePutRequest(BUCKET_NAME, null);

    assertNull(response, "Handler should return null without ?acl param");
  }

  private static Stream<String> grantHeaderNames() {
    return Stream.of(
        S3Acl.GRANT_READ,
        S3Acl.GRANT_WRITE,
        S3Acl.GRANT_READ_ACP,
        S3Acl.GRANT_WRITE_ACP,
        S3Acl.GRANT_FULL_CONTROL
    );
  }

  @ParameterizedTest
  @MethodSource("grantHeaderNames")
  public void testHandlePutRequestWithGrantHeaders(String headerName) throws Exception {
    when(headers.getHeaderString(headerName))
        .thenReturn("id=\"testuser\"");

    assertSucceeds(() -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testHandlePutRequestWithMultipleHeaders() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser1\"");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn("id=\"testuser2\"");

    assertSucceeds(() -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testHandlePutRequestWithUnsupportedGranteeType() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("uri=\"http://example.com\"");

    assertErrorResponse(NOT_IMPLEMENTED,
        () -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testHandlePutRequestWithEmailAddressType() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("emailAddress=\"test@example.com\"");

    assertErrorResponse(NOT_IMPLEMENTED,
        () -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testHandlePutRequestBucketNotFound() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    assertThrows(OS3Exception.class,
        () -> aclHandler.handlePutRequest("nonexistent-bucket", null),
        "Should throw OS3Exception for non-existent bucket");
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

    assertSucceeds(() -> aclHandler.handlePutRequest(BUCKET_NAME, body));
  }

  @Test
  public void testHandlePutRequestWithInvalidHeaderFormat() {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("invalid-format");

    assertThrows(OS3Exception.class,
        () -> aclHandler.handlePutRequest(BUCKET_NAME, null),
        "Should throw OS3Exception for invalid header format");
  }

  @Test
  public void testHandlePutRequestWithMultipleGrantees() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"user1\",id=\"user2\"");

    assertSucceeds(() -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testPutAclReplacesExistingAcls() throws Exception {
    // Set initial ACL
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"user1\"");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn(null);

    aclHandler.handlePutRequest(BUCKET_NAME, null);

    // Replace with new ACL
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(null);
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn("id=\"user2\"");

    assertSucceeds(() -> aclHandler.handlePutRequest(BUCKET_NAME, null));
  }

  @Test
  public void testAuditLoggingOnBucketNotFound() throws Exception {
    BucketAclHandler spyHandler = spy(aclHandler);

    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");

    // This should throw exception for non-existent bucket
    assertThrows(OS3Exception.class,
        () -> spyHandler.handlePutRequest("nonexistent-bucket", null));

    // Verify that auditWriteFailure was called with PUT_ACL action
    verify(spyHandler, times(1)).auditWriteFailure(
        eq(S3GAction.PUT_ACL),
        any(OS3Exception.class));
  }

  @Test
  public void testAuditLoggingOnInvalidArgument() throws Exception {
    BucketAclHandler spyHandler = spy(aclHandler);

    // Invalid format will trigger OS3Exception
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("invalid-format");

    assertThrows(OS3Exception.class,
        () -> spyHandler.handlePutRequest(BUCKET_NAME, null));

    // Verify that auditWriteFailure was called with PUT_ACL action
    verify(spyHandler, times(1)).auditWriteFailure(
        eq(S3GAction.PUT_ACL),
        any(OS3Exception.class));
  }

  // ===== GET Request Tests =====

  @Test
  public void testHandleGetRequestWithAclQueryParam() throws Exception {
    assertNotNull(aclHandler.handleGetRequest(BUCKET_NAME),
        "Handler should handle request with ?acl param");
  }

  @Test
  public void testHandleGetRequestWithoutAclQueryParam() throws Exception {
    // Remove "acl" query parameter - handler should not handle request
    aclHandler.queryParamsForTest().unset("acl");

    assertNull(aclHandler.handleGetRequest(BUCKET_NAME),
        "Handler should return null without ?acl param");
  }

  @Test
  public void testHandleGetRequestSucceeds() throws Exception {
    assertSucceeds(() -> aclHandler.handleGetRequest(BUCKET_NAME));
  }

  @Test
  public void testHandleGetRequestBucketNotFound() {
    assertThrows(OS3Exception.class,
        () -> aclHandler.handleGetRequest("nonexistent-bucket"),
        "Should throw OS3Exception for non-existent bucket");
  }

  @Test
  public void testHandleGetRequestReturnsCorrectAclStructure() throws Exception {
    // First set some ACL
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=\"testuser\"");
    aclHandler.handlePutRequest(BUCKET_NAME, null);

    // Now get ACL
    Response response = aclHandler.handleGetRequest(BUCKET_NAME);

    assertNotNull(response);
    S3BucketAcl result = (S3BucketAcl) response.getEntity();
    assertNotNull(result.getOwner());
    assertNotNull(result.getAclList());
    assertNotNull(result.getAclList().getGrantList());
  }

  @Test
  public void testAuditLoggingOnGetSuccess() throws Exception {
    BucketAclHandler spyHandler = spy(aclHandler);

    Response response = spyHandler.handleGetRequest(BUCKET_NAME);

    assertNotNull(response);
    // Verify that auditReadSuccess was called with GET_ACL action
    verify(spyHandler, times(1)).auditReadSuccess(eq(S3GAction.GET_ACL));
  }

  @Test
  public void testAuditLoggingOnGetBucketNotFound() throws Exception {
    BucketAclHandler spyHandler = spy(aclHandler);

    // This should throw exception for non-existent bucket
    assertThrows(OS3Exception.class,
        () -> spyHandler.handleGetRequest("nonexistent-bucket"));

    // Verify that auditReadFailure was called with GET_ACL action
    verify(spyHandler, times(1)).auditReadFailure(
        eq(S3GAction.GET_ACL),
        any(OS3Exception.class));
  }
}
