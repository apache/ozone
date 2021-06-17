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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Test operation permission check result.
 */
public class TestPermissionCheck {
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ObjectStore objectStore;
  private OzoneBucket bucket;
  private OzoneVolume volume;
  private OMException exception;
  private HttpHeaders headers;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_S3_VOLUME_NAME,
        OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    client = Mockito.mock(OzoneClient.class);
    objectStore = Mockito.mock(ObjectStore.class);
    bucket = Mockito.mock(OzoneBucket.class);
    volume = Mockito.mock(OzoneVolume.class);
    exception = new OMException("Permission Denied",
        OMException.ResultCodes.PERMISSION_DENIED);
    Mockito.when(client.getObjectStore()).thenReturn(objectStore);
    Mockito.when(client.getConfiguration()).thenReturn(conf);
    headers = Mockito.mock(HttpHeaders.class);
  }

  /**
   *  Root Endpoint.
   */
  @Test
  public void testListS3Buckets() throws IOException {
    doThrow(exception).when(objectStore).getVolume(anyString());
    RootEndpoint rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(client);

    try {
      rootEndpoint.get();
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  /**
   *  Bucket Endpoint.
   */
  @Test
  public void testGetBucket() throws IOException {
    doThrow(exception).when(objectStore).getS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    try {
      bucketEndpoint.head("bucketName");
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testCreateBucket() throws IOException {
    Mockito.when(objectStore.getVolume(anyString())).thenReturn(volume);
    doThrow(exception).when(objectStore).createS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    try {
      bucketEndpoint.put("bucketName", null, null, null);
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testDeleteBucket() throws IOException {
    doThrow(exception).when(objectStore).deleteS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    try {
      bucketEndpoint.delete("bucketName");
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }
  @Test
  public void testListMultiUpload() throws IOException {
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).listMultipartUploads(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    try {
      bucketEndpoint.listMultipartUploads("bucketName", "prefix");
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testListKey() throws IOException {
    Mockito.when(objectStore.getVolume(anyString())).thenReturn(volume);
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).listKeys(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    try {
      bucketEndpoint.get("bucketName", null, null, null, 1000,
          null, null, null, null, null, null, null);
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testDeleteKeys() throws IOException, OS3Exception {
    Mockito.when(objectStore.getVolume(anyString())).thenReturn(volume);
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).deleteKey(any());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    MultiDeleteRequest request = new MultiDeleteRequest();
    List<MultiDeleteRequest.DeleteObject> objectList = new ArrayList<>();
    objectList.add(new MultiDeleteRequest.DeleteObject("deleteKeyName"));
    request.setQuiet(false);
    request.setObjects(objectList);

    MultiDeleteResponse response =
        bucketEndpoint.multiDelete("BucketName", "keyName", request);
    Assert.assertTrue(response.getErrors().size() == 1);
    Assert.assertTrue(
        response.getErrors().get(0).getCode().equals("PermissionDenied"));
  }

  @Test
  public void testGetAcl() throws Exception {
    Mockito.when(objectStore.getVolume(anyString())).thenReturn(volume);
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).getAcls();

    HttpServletRequest servletRequest = Mockito.mock(HttpServletRequest.class);
    Map<String, String[]> parameterMap = Mockito.mock(Map.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    when(parameterMap.containsKey("acl")).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    try {
      bucketEndpoint.get("bucketName", null, null, null, 1000,
          null, null, null, null, null, "acl", null);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception &&
          ((OS3Exception)e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testSetAcl() throws Exception {
    Mockito.when(objectStore.getVolume(anyString())).thenReturn(volume);
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).addAcl(any());

    HttpServletRequest servletRequest = Mockito.mock(HttpServletRequest.class);
    Map<String, String[]> parameterMap = Mockito.mock(Map.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    when(parameterMap.containsKey("acl")).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    try {
      bucketEndpoint.put("bucketName", "acl", headers, null);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception &&
          ((OS3Exception)e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  /**
   *  Object Endpoint.
   */
  @Test
  public void testGetKey() throws IOException {
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).getKey(anyString());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);

    try {
      objectEndpoint.get("bucketName", "keyPath", null, 1000, "marker",
          null);
      Assert.fail("Should fail");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testPutKey() throws IOException {
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket)
        .createKey(anyString(), anyLong(), any(), any(), any());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);

    try {
      objectEndpoint.put("bucketName", "keyPath", 1024, 0, null, null);
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testDeleteKey() throws IOException {
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).deleteKey(anyString());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);

    try {
      objectEndpoint.delete("bucketName", "keyPath", null);
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testMultiUploadKey() throws IOException {
    Mockito.when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket)
        .initiateMultipartUpload(anyString(), any(), any());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);

    try {
      objectEndpoint.initializeMultipartUpload("bucketName", "keyPath");
      Assert.fail("Should fail");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof OS3Exception);
      Assert.assertTrue(((OS3Exception) e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }
}
