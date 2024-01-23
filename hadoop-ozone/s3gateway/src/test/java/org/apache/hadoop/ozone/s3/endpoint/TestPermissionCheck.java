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
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test operation permission check result.
 * Test-cases in this class verify that permission denied errors from OM
 * are handled correctly by S3 endpoints.
 */
public class TestPermissionCheck {
  private OzoneConfiguration conf;
  private OzoneClient client;
  private ClientProtocol clientProtocol;
  private OzoneBucket bucket;
  private ObjectStore objectStore;
  private OzoneVolume volume;
  private OMException exception;
  private HttpHeaders headers;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_S3_VOLUME_NAME,
        OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    client = mock(OzoneClient.class);
    objectStore = mock(ObjectStore.class);
    bucket = mock(OzoneBucket.class);
    volume = mock(OzoneVolume.class);
    when(volume.getName()).thenReturn("s3Volume");
    exception = new OMException("Permission Denied",
        OMException.ResultCodes.PERMISSION_DENIED);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getConfiguration()).thenReturn(conf);
    headers = mock(HttpHeaders.class);
    clientProtocol = mock(ClientProtocol.class);
    S3GatewayMetrics.create(conf);
    when(client.getProxy()).thenReturn(clientProtocol);
  }

  /**
   *  Root Endpoint.
   */
  @Test
  public void testListS3Buckets() throws IOException {
    doThrow(exception).when(objectStore).getS3Volume();
    RootEndpoint rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(client);
    OS3Exception e = assertThrows(OS3Exception.class, () -> rootEndpoint.get());
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  /**
   *  Bucket Endpoint.
   */
  @Test
  public void testGetBucket() throws IOException {
    doThrow(exception).when(objectStore).getS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.head("bucketName"));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testCreateBucket() throws IOException {
    when(objectStore.getVolume(anyString())).thenReturn(volume);
    doThrow(exception).when(objectStore).createS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.put("bucketName", null, null, null));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testDeleteBucket() throws IOException {
    doThrow(exception).when(objectStore).deleteS3Bucket(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.delete("bucketName"));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }
  @Test
  public void testListMultiUpload() throws IOException {
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).listMultipartUploads(anyString());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.listMultipartUploads("bucketName", "prefix"));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testListKey() throws IOException {
    when(objectStore.getVolume(anyString())).thenReturn(volume);
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).listKeys(anyString(), isNull(),
        anyBoolean());
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);

    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get(
        "bucketName", null, null, null, 1000,
        null, null, null, null, null, null));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testDeleteKeys() throws IOException, OS3Exception {
    when(objectStore.getVolume(anyString())).thenReturn(volume);
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
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
    assertEquals(1, response.getErrors().size());
    assertEquals("PermissionDenied", response.getErrors().get(0).getCode());
  }

  @Test
  public void testGetAcl() throws Exception {
    when(objectStore.getS3Volume()).thenReturn(volume);
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).getAcls();

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    Map<String, String[]> parameterMap = mock(Map.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    when(parameterMap.containsKey("acl")).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get(
        "bucketName", null, null, null, 1000, null, null, null, null, "acl",
        null), "Expected OS3Exception with FORBIDDEN http code.");
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testSetAcl() throws Exception {
    when(objectStore.getS3Volume()).thenReturn(volume);
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).addAcl(any());

    HttpServletRequest servletRequest = mock(HttpServletRequest.class);
    Map<String, String[]> parameterMap = mock(Map.class);
    when(servletRequest.getParameterMap()).thenReturn(parameterMap);

    when(parameterMap.containsKey("acl")).thenReturn(true);
    when(headers.getHeaderString(S3Acl.GRANT_READ))
        .thenReturn(S3Acl.ACLIdentityType.USER.getHeaderType() + "=root");
    BucketEndpoint bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(client);
    try {
      bucketEndpoint.put("bucketName", "acl", headers, null);
    } catch (Exception e) {
      assertTrue(e instanceof OS3Exception &&
          ((OS3Exception)e).getHttpCode() == HTTP_FORBIDDEN);
    }
  }

  /**
   *  Object Endpoint.
   */
  @Test
  public void testGetKey() throws IOException {
    when(client.getProxy()).thenReturn(clientProtocol);
    doThrow(exception).when(clientProtocol)
        .getS3KeyDetails(anyString(), anyString());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);
    objectEndpoint.setOzoneConfiguration(conf);

    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.get(
        "bucketName", "keyPath", 0, null, 1000, "marker"));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testPutKey() throws IOException {
    when(objectStore.getS3Volume()).thenReturn(volume);
    when(volume.getBucket("bucketName")).thenReturn(bucket);
    doThrow(exception).when(clientProtocol).createKey(
            anyString(), anyString(), anyString(), anyLong(), any(), any());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);
    objectEndpoint.setOzoneConfiguration(conf);

    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
        "bucketName", "keyPath", 1024, 0, null,
        new ByteArrayInputStream(new byte[]{})));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testDeleteKey() throws IOException {
    when(objectStore.getS3Volume()).thenReturn(volume);
    doThrow(exception).when(clientProtocol).deleteKey(anyString(), anyString(),
        anyString(), anyBoolean());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);
    objectEndpoint.setOzoneConfiguration(conf);

    OS3Exception e = assertThrows(OS3Exception.class, () ->
        objectEndpoint.delete("bucketName", "keyPath", null));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }

  @Test
  public void testMultiUploadKey() throws IOException {
    when(objectStore.getS3Bucket(anyString())).thenReturn(bucket);
    doThrow(exception).when(bucket).initiateMultipartUpload(anyString(), any());
    ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(client);
    objectEndpoint.setHeaders(headers);
    objectEndpoint.setOzoneConfiguration(conf);

    OS3Exception e = assertThrows(OS3Exception.class, () ->
        objectEndpoint.initializeMultipartUpload("bucketName", "keyPath"));
    assertEquals(HTTP_FORBIDDEN, e.getHttpCode());
  }
}
