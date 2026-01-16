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
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests Bucket ACL get/set request.
 */
public class TestBucketAcl {

  private static final String BUCKET_NAME = OzoneConsts.S3_BUCKET;
  private OzoneClient client;

  private Map<String, String[]> parameterMap;
  private HttpHeaders headers;
  private BucketEndpoint bucketEndpoint;
  private static final String ACL_MARKER = "acl";

  @BeforeEach
  public void setup() throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    parameterMap = mock(Map.class);
    headers = mock(HttpHeaders.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
    bucketEndpoint.queryParamsForTest().set(QueryParams.ACL, ACL_MARKER);
  }

  @AfterEach
  public void clean() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  /**
   * Helper method to get ACL from bucket and validate response.
   */
  private S3BucketAcl getBucketAcl(String bucketName) throws Exception {
    Response response = bucketEndpoint.get(bucketName);
    assertEquals(HTTP_OK, response.getStatus());
    return assertInstanceOf(S3BucketAcl.class, response.getEntity());
  }

  @Test
  public void testGetAcl() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    Response response = bucketEndpoint.get(BUCKET_NAME);
    assertEquals(HTTP_OK, response.getStatus());
    System.out.println(response.getEntity());
  }

  @Test
  public void testSetAclWithNotSupportedGranteeType() throws Exception {
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.GROUP.getHeaderType() + "=root");
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.put(BUCKET_NAME, null));
    assertEquals(e.getHttpCode(), HTTP_NOT_IMPLEMENTED);
  }

  @Test
  public void testRead() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.READ.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testWrite() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testReadACP() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ_ACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.READ_ACP.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testWriteACP() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_WRITE_ACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE_ACP.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testFullControl() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.FULL_CONTROL.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testCombination() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.GRANT_READ_ACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.GRANT_WRITE_ACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.GRANT_FULL_CONTROL))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(5, getResponse.getAclList().getGrantList().size());
  }

  @Test
  public void testPutClearOldAcls() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    // Put READ
    Response response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.READ.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
    OzoneVolume volume = bucketEndpoint.getVolume();
    assertEquals(1, volume.getAcls().size());
    assertEquals(IAccessAuthorizer.ACLType.READ,
        volume.getAcls().get(0).getAclList().get(0));

    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(null);
    when(headers.getHeaderString(S3Acl.GRANT_WRITE))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    //Put WRITE
    response =
        bucketEndpoint.put(BUCKET_NAME, null);
    assertEquals(HTTP_OK, response.getStatus());
    getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
    volume = bucketEndpoint.getVolume();
    assertEquals(1, volume.getAcls().size());
    assertEquals(IAccessAuthorizer.ACLType.READ,
        volume.getAcls().get(0).getAclList().get(0));
  }

  @Test
  public void testAclInBodyWithGroupUser() {
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("groupAccessControlList.xml");
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    assertThrows(OS3Exception.class, () -> bucketEndpoint.put(
        BUCKET_NAME, inputBody));
  }

  @Test
  public void testAclInBody() throws Exception {
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    Response response =
        bucketEndpoint.put(BUCKET_NAME, inputBody);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = getBucketAcl(BUCKET_NAME);
    assertEquals(2, getResponse.getAclList().getGrantList().size());
  }

  @Test
  public void testBucketNotExist() throws Exception {
    when(parameterMap.containsKey(ACL_MARKER)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.get("bucket-not-exist"));
    assertEquals(e.getHttpCode(), HTTP_NOT_FOUND);
  }
}
