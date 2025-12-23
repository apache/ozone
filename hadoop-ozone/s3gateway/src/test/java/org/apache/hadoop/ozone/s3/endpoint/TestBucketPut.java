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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test Create Bucket functionality.
 */
public class TestBucketPut {

  private String bucketName = OzoneConsts.BUCKET;
  private BucketEndpoint bucketEndpoint;
  private HttpHeaders mockHeaders;
  private static final String VALID_ACL_XML =
      "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\" " +
          "                      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
          "  <Owner>" +
          "    <ID>owner-id</ID>" +
          "    <DisplayName>owner-name</DisplayName>" +
          "  </Owner>" +
          "  <AccessControlList>" +
          "    <Grant>" +
          "      <Grantee xsi:type=\"CanonicalUser\">" +
          "        <ID>owner-id</ID>" +
          "        <DisplayName>owner-name</DisplayName>" +
          "      </Grantee>" +
          "      <Permission>FULL_CONTROL</Permission>" +
          "    </Grant>" +
          "  </AccessControlList>" +
          "</AccessControlPolicy>";
  private static final String WHITESPACE_ONLY = "  ";

  @BeforeEach
  public void setup() throws Exception {

    //Create client stub and object store stub.
    OzoneClient clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();

    mockHeaders = mock(HttpHeaders.class);
    bucketEndpoint.setHeaders(mockHeaders);
  }

  @Test
  public void testAclWithMissingHeaders() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    InputStream body = new ByteArrayInputStream(VALID_ACL_XML.getBytes(StandardCharsets.UTF_8));

    Response resp = bucketEndpoint.put(bucketName, body);
    assertEquals(HTTP_OK, resp.getStatus());
  }

  @Test
  public void testAclWithMissingHeadersAndNoBody() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    WebApplicationException wae = assertThrows(WebApplicationException.class,
        () -> bucketEndpoint.put(bucketName, null));

    Throwable cause = wae.getCause();
    assertNotNull(cause);
    assertTrue(cause instanceof OS3Exception);

    OS3Exception os3 = (OS3Exception) cause;

    assertEquals(INVALID_REQUEST.getCode(), os3.getCode());
    assertEquals(HTTP_BAD_REQUEST, os3.getHttpCode());
  }

  @Test
  public void testBucketFailWithAuthHeaderMissing() throws Exception {
    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }

  @Test
  public void testBucketPut() throws Exception {
    Response response = bucketEndpoint.put(bucketName, null);
    assertEquals(HTTP_OK, response.getStatus());
    assertNotNull(response.getLocation());

    // Create-bucket on an existing bucket fails
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.put(
        bucketName, null));
    assertEquals(BUCKET_ALREADY_EXISTS.getCode(), e.getCode());
    assertEquals(HTTP_CONFLICT, e.getHttpCode());
  }

  @Test
  public void testPutAclOnNonExistingBucket() throws Exception {
    OS3Exception e = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.put(bucketName, null));
    assertEquals(NO_SUCH_BUCKET.getCode(), e.getCode());
    assertEquals(HTTP_NOT_FOUND, e.getHttpCode());
  }

  @Test
  public void testPutAclWithGrantFullControlHeader() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    when(mockHeaders.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn("id=\"owner-id\"");

    Response resp = bucketEndpoint.put(bucketName, null);

    assertEquals(HTTP_OK, resp.getStatus());
  }

  @Test
  public void testPutAclWithInvalidXmlBody() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    InputStream body = new ByteArrayInputStream(
        "not-xml".getBytes(StandardCharsets.UTF_8));

    WebApplicationException wae = assertThrows(WebApplicationException.class,
        () -> bucketEndpoint.put(bucketName, body));

    Throwable cause = wae.getCause();
    assertNotNull(cause);
    assertTrue(cause instanceof OS3Exception);
    OS3Exception os3 = (OS3Exception) cause;

    assertEquals(INVALID_REQUEST.getCode(), os3.getCode());
    assertEquals(HTTP_BAD_REQUEST, os3.getHttpCode());
  }

  @Test
  public void testPutAclWithMalformedGrantHeader() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    when(mockHeaders.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn("id\"owner-id\"");

    OS3Exception os3 = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.put(bucketName, null));

    assertEquals(INVALID_ARGUMENT.getCode(), os3.getCode());
    assertEquals(HTTP_BAD_REQUEST, os3.getHttpCode());
  }

  @Test
  public void testPutAclWithBothHeadersAndBody() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    // Header: READ
    when(mockHeaders.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn("id=owner-id");

    // Body: FULL_CONTROL
    InputStream body = new ByteArrayInputStream(
        VALID_ACL_XML.getBytes(StandardCharsets.UTF_8));

    Response resp = bucketEndpoint.put(bucketName, body);
    assertEquals(HTTP_OK, resp.getStatus());

    OzoneBucket bucket = bucketEndpoint.getClient()
         .getObjectStore()
         .getS3Bucket(bucketName);

    List<OzoneAcl> acls = bucket.getAcls();
    assertFalse(acls.isEmpty());

    OzoneAcl ownerAcl = acls.stream()
         .filter(acl -> "owner-id".equals(acl.getName())
             && acl.getAclScope() == ACCESS)
         .findFirst()
         .orElseThrow(() -> new AssertionError("owner-id ACL not found"));

    assertEquals("owner-id", ownerAcl.getName());

    List<IAccessAuthorizer.ACLType> permissions = ownerAcl.getAclList();

    assertTrue(permissions.contains(IAccessAuthorizer.ACLType.READ),
        "Expected READ permission from header");

    assertFalse(permissions.contains(IAccessAuthorizer.ACLType.ALL),
        "FULL_CONTROL/ALL from body should not be applied when header is present");
  }

  @Test
  public void testBucketFailWithInvalidHeader() throws Exception {
    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }

  @Test
  public void testPutAclWithEmptyGrantHeaderValue() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    when(mockHeaders.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn(""); // empty

    Response resp = bucketEndpoint.put(bucketName, null);

    assertEquals(HTTP_OK, resp.getStatus());
  }

  @Test
  public void testPutAclWithWhitespaceGrantHeaderValue() throws Exception {
    bucketEndpoint.getClient().getObjectStore().createS3Bucket(bucketName);

    when(mockHeaders.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn(WHITESPACE_ONLY); // whitespace only

    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> bucketEndpoint.put(bucketName, null));

    assertEquals(INVALID_ARGUMENT.getCode(), ex.getCode());
    assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
  }
}
