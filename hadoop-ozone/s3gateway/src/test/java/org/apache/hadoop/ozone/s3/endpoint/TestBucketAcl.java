/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * This class tests Bucket ACL get/set request.
 */
public class TestBucketAcl {

  private static final String bucketName = OzoneConsts.S3_BUCKET;
  private OzoneClient client;

  private HttpServletRequest servletRequest;
  private Map<String, String[]> parameterMap;
  private HttpHeaders headers;
  private BucketEndpoint bucketEndpoint;
  private static final String aclMarker = "acl";

  @Before
  public void setup() throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucketName);

    servletRequest = Mockito.mock(HttpServletRequest.class);
    parameterMap = Mockito.mock(Map.class);
    headers = Mockito.mock(HttpHeaders.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
  }

  @After
  public void clean() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testGetAcl() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    Response response =
        bucketEndpoint.get(bucketName, null, null, null, 0, null, null,
            null, null, null, aclMarker, headers);
    assertEquals(HTTP_OK, response.getStatus());
    System.out.println(response.getEntity());
  }

  @Test
  public void testSetAclWithNotSupportedGranteeType() throws Exception {
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(S3Acl.ACLIdentityType.GROUP.getHeaderType() + "=root");
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    try {
      bucketEndpoint.put(bucketName, aclMarker, headers, null);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception &&
          ((OS3Exception) e).getHttpCode() == HTTP_NOT_IMPLEMENTED);
    }
  }

  @Test
  public void testRead() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.READ.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testWrite() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantWrite))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testReadACP() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantReadACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse =
        bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.READ_ACP.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testWriteACP() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantWriteACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE_ACP.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testFullControl() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantFullControl))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.FULL_CONTROL.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test
  public void testCombination() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.grantWrite))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.grantReadACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.grantWriteACP))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    when(headers.getHeaderString(S3Acl.grantFullControl))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(5, getResponse.getAclList().getGrantList().size());
  }

  @Test
  public void testPutClearOldAcls() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(null);
    when(headers.getHeaderString(S3Acl.grantWrite))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    response =
        bucketEndpoint.put(bucketName, aclMarker, headers, null);
    assertEquals(HTTP_OK, response.getStatus());
    getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(1, getResponse.getAclList().getGrantList().size());
    assertEquals(S3Acl.ACLType.WRITE.getValue(),
        getResponse.getAclList().getGrantList().get(0).getPermission());
  }

  @Test(expected = OS3Exception.class)
  public void testAclInBodyWithGroupUser() throws Exception {
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "  <Owner>\n" +
                "    <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>\n" +
                "    <DisplayName>owner</DisplayName>\n" +
                "  </Owner>\n" +
                "  <AccessControlList>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\n" +
                "        <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>\n" +
                "        <DisplayName>owner</DisplayName>\n" +
                "      </Grantee>\n" +
                "      <Permission>FULL_CONTROL</Permission>\n" +
                "    </Grant>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\n" +
                "        <URI xmlns=\"\">http://acs.amazonaws.com/groups/global/AllUsers</URI>\n" +
                "      </Grantee>\n" +
                "      <Permission xmlns=\"\">READ</Permission>\n" +
                "    </Grant>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">\n" +
                "        <URI xmlns=\"\">http://acs.amazonaws.com/groups/s3/LogDelivery</URI>\n" +
                "      </Grantee>\n" +
                "      <Permission xmlns=\"\">WRITE</Permission>\n" +
                "    </Grant>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"AmazonCustomerByEmail\">\n" +
                "        <EmailAddress xmlns=\"\">xyz@amazon.com</EmailAddress>\n" +
                "      </Grantee>\n" +
                "      <Permission xmlns=\"\">WRITE_ACP</Permission>\n" +
                "    </Grant>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\n" +
                "        <ID xmlns=\"\">f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID</ID>\n" +
                "      </Grantee>\n" +
                "      <Permission xmlns=\"\">READ_ACP</Permission>\n" +
                "    </Grant>\n" +
                "  </AccessControlList>\n" +
                "</AccessControlPolicy>\n").getBytes(UTF_8));

    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    bucketEndpoint.put(bucketName, aclMarker, headers, inputBody);
  }

  @Test
  public void testAclInBody() throws Exception {
    ByteArrayInputStream inputBody =
        new ByteArrayInputStream(
            ("<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
                "  <Owner>\n" +
                "    <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>\n" +
                "    <DisplayName>owner</DisplayName>\n" +
                "  </Owner>\n" +
                "  <AccessControlList>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\n" +
                "        <ID>852b113e7a2f25102679df27bb0ae12b3f85be6BucketOwnerCanonicalUserID</ID>\n" +
                "        <DisplayName>owner</DisplayName>\n" +
                "      </Grantee>\n" +
                "      <Permission>FULL_CONTROL</Permission>\n" +
                "    </Grant>\n" +
                "    <Grant>\n" +
                "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">\n" +
                "        <ID xmlns=\"\">f30716ab7115dcb44a5ef76e9d74b8e20567f63TestAccountCanonicalUserID</ID>\n" +
                "      </Grantee>\n" +
                "      <Permission xmlns=\"\">READ_ACP</Permission>\n" +
                "    </Grant>\n" +
                "  </AccessControlList>\n" +
                "</AccessControlPolicy>\n").getBytes(UTF_8));

    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    Response response =
        bucketEndpoint.put(bucketName, aclMarker, headers, inputBody);
    assertEquals(HTTP_OK, response.getStatus());
    S3BucketAcl getResponse = bucketEndpoint.getAcl(bucketName);
    assertEquals(2, getResponse.getAclList().getGrantList().size());
  }

  @Test
  public void testBucketNotExist() throws Exception {
    when(parameterMap.containsKey(aclMarker)).thenReturn(true);
    when(headers.getHeaderString(S3Acl.grantRead))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    try {
      bucketEndpoint.getAcl("bucket-not-exist");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception &&
          ((OS3Exception)e).getHttpCode() == HTTP_NOT_FOUND);
    }
  }
}
