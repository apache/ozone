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
package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.BucketEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.RootEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.TestBucketAcl;
import org.apache.hadoop.ozone.s3.endpoint.MultipartUploadInitiateResponse;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link S3GatewayMetrics}.
 */
public class TestS3GatewayMetrics {

  private String bucketName = OzoneConsts.BUCKET;
  private String keyName = OzoneConsts.KEY;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;
  private RootEndpoint rootEndpoint;
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;
  private HttpHeaders headers;
  private static final String ACL_MARKER = "acl";
  private static final String CONTENT = "0123456789";
  private S3GatewayMetrics metrics;
  private ContainerRequestContext context;


  @Before
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);

    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);

    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);

    keyEndpoint = new ObjectEndpoint();
    keyEndpoint.setClient(clientStub);
    keyEndpoint.setOzoneConfiguration(new OzoneConfiguration());

    headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    keyEndpoint.setHeaders(headers);
    metrics = bucketEndpoint.getMetrics();

    context = Mockito.mock(ContainerRequestContext.class);
    Mockito.when(context.getUriInfo()).thenReturn(Mockito.mock(UriInfo.class));
    Mockito.when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());
    keyEndpoint.setContext(context);
  }

  /**
   * Bucket Level Endpoints.
   */

  @Test
  public void testHeadBucketSuccess() throws Exception {

    long oriMetric = metrics.getHeadBucketSuccess();

    bucketEndpoint.head(bucketName);

    long curMetric = metrics.getHeadBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListBucketSuccess() throws Exception {

    long oriMetric = metrics.getListS3BucketsSuccess();

    rootEndpoint.get().getEntity();

    long curMetric = metrics.getListS3BucketsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketSuccess() throws Exception {
    long oriMetric = metrics.getGetBucketSuccess();

    clientStub = createClientWithKeys("file1");
    bucketEndpoint.setClient(clientStub);
    bucketEndpoint.get(bucketName, null,
        null, null, 1000, null,
        null, "random", null,
        null, null).getEntity();

    long curMetric = metrics.getGetBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketFailure() throws Exception {
    long oriMetric = metrics.getGetBucketFailure();

    try {
      // Searching for a bucket that does not exist
      bucketEndpoint.get("newBucket", null,
          null, null, 1000, null,
          null, "random", null,
          null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }

    long curMetric = metrics.getGetBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketSuccess() throws Exception {

    long oriMetric = metrics.getCreateBucketSuccess();

    bucketEndpoint.put(bucketName, null,
        null, null);
    long curMetric = metrics.getCreateBucketSuccess();

    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketFailure() throws Exception {
    // Creating an error by trying to create a bucket that already exists
    long oriMetric = metrics.getCreateBucketFailure();

    bucketEndpoint.put(bucketName, null, null, null);

    long curMetric = metrics.getCreateBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteBucketSuccess() throws Exception {
    long oriMetric = metrics.getDeleteBucketSuccess();

    bucketEndpoint.delete(bucketName);

    long curMetric = metrics.getDeleteBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteBucketFailure() throws Exception {
    long oriMetric = metrics.getDeleteBucketFailure();
    bucketEndpoint.delete(bucketName);
    try {
      // Deleting a bucket that does not exist will result in delete failure
      bucketEndpoint.delete(bucketName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }

    long curMetric = metrics.getDeleteBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclSuccess() throws Exception {
    long oriMetric = metrics.getGetAclSuccess();

    Response response =
        bucketEndpoint.get(bucketName, null, null,
            null, 0, null, null,
            null, null, "acl", null);
    long curMetric = metrics.getGetAclSuccess();
    assertEquals(HTTP_OK, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclFailure() throws Exception {
    long oriMetric = metrics.getGetAclFailure();
    try {
      // Failing the getACL endpoint by applying ACL on a non-Existent Bucket
      bucketEndpoint.get("random_bucket", null,
          null, null, 0, null,
          null, null, null, "acl", null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }
    long curMetric = metrics.getGetAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclSuccess() throws Exception {
    long oriMetric = metrics.getPutAclSuccess();

    clientStub.getObjectStore().createS3Bucket("b1");
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    bucketEndpoint.put("b1", ACL_MARKER, headers, inputBody);
    inputBody.close();
    long curMetric = metrics.getPutAclSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclFailure() throws Exception {
    // Failing the putACL endpoint by applying ACL on a non-Existent Bucket
    long oriMetric = metrics.getPutAclFailure();

    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    try {
      bucketEndpoint.put("unknown_bucket", ACL_MARKER, headers,
          inputBody);
      fail();
    } catch (OS3Exception ex) {
    } finally {
      inputBody.close();
    }
    long curMetric = metrics.getPutAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }


  /**
   * Object Level Endpoints.
   */

  @Test
  public void testHeadKeySuccess() throws Exception {
    bucket.createKey(keyName, 0).close();

    long oriMetric = metrics.getHeadKeySuccess();

    keyEndpoint.head(bucketName, keyName);

    long curMetric = metrics.getHeadKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testHeadKeyFailure() throws Exception {
    long oriMetric = metrics.getHeadKeyFailure();

    keyEndpoint.head(bucketName, "unknownKey");

    long curMetric = metrics.getHeadKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateKeySuccess() throws Exception {

    long oriMetric = metrics.getCreateKeySuccess();
    // Create an input stream
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Create the file
    keyEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);
    body.close();
    long curMetric = metrics.getCreateKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateKeyFailure() throws Exception {
    long oriMetric = metrics.getCreateKeyFailure();

    // Create the file in a bucket that does not exist
    try {
      keyEndpoint.put("unknownBucket", keyName, CONTENT
          .length(), 1, null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCreateKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }


  @Test
  public void testDeleteKeySuccess() throws Exception {
    long oriMetric = metrics.getDeleteKeySuccess();

    bucket.createKey(keyName, 0).close();
    keyEndpoint.delete(bucketName, keyName, null);
    long curMetric = metrics.getDeleteKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteKeyFailure() throws Exception {
    long oriMetric = metrics.getDeleteKeyFailure();
    try {
      keyEndpoint.delete("unknownBucket", keyName, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getDeleteKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetKeySuccess() throws Exception {
    long oriMetric = metrics.getGetKeySuccess();

    // Create an input stream
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Create the file
    keyEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);
    // GET the key from the bucket
    keyEndpoint.get(bucketName, keyName, null, 0,
        null, body);
    long curMetric = metrics.getGetKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetKeyFailure() throws Exception {
    long oriMetric = metrics.getGetKeyFailure();
    // Fetching a non-existent key
    try {
      keyEndpoint.get(bucketName, "unknownKey", null, 0,
          null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());
    }
    long curMetric = metrics.getGetKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testInitMultiPartUploadSuccess() throws Exception {

    long oriMetric = metrics.getInitMultiPartUploadSuccess();
    keyEndpoint.initializeMultipartUpload(bucketName, keyName);
    long curMetric = metrics.getInitMultiPartUploadSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testInitMultiPartUploadFailure() throws Exception {
    long oriMetric = metrics.getInitMultiPartUploadFailure();
    try {
      keyEndpoint.initializeMultipartUpload("unknownBucket", keyName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
    long curMetric = metrics.getInitMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadSuccess() throws Exception {

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    long oriMetric = metrics.getAbortMultiPartUploadSuccess();

    // Abort the Upload Successfully by deleting the key using the Upload-Id
    keyEndpoint.delete(bucketName, keyName, uploadID);

    long curMetric = metrics.getAbortMultiPartUploadSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadFailure() throws Exception {
    long oriMetric = metrics.getAbortMultiPartUploadFailure();

    // Fail the Abort Method by providing wrong uploadID
    try {
      keyEndpoint.delete(bucketName, keyName, "wrongId");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getAbortMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }


  @Test
  public void testCompleteMultiPartUploadSuccess() throws Exception {

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    long oriMetric = metrics.getCompleteMultiPartUploadSuccess();
    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    Response response = keyEndpoint.completeMultipartUpload(bucketName, keyName,
        uploadID, completeMultipartUploadRequest);
    long curMetric = metrics.getCompleteMultiPartUploadSuccess();
    assertEquals(200, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCompleteMultiPartUploadFailure() throws Exception {
    long oriMetric = metrics.getCompleteMultiPartUploadFailure();
    CompleteMultipartUploadRequest completeMultipartUploadRequestNew = new
        CompleteMultipartUploadRequest();
    try {
      keyEndpoint.completeMultipartUpload(bucketName, "key2",
          "random", completeMultipartUploadRequestNew);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCompleteMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateMultipartKeySuccess() throws Exception {

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    long oriMetric = metrics.getCreateMultipartKeySuccess();
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.put(bucketName, keyName, CONTENT.length(),
        1, uploadID, body);
    long curMetric = metrics.getCreateMultipartKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateMultipartKeyFailure() throws Exception {
    long oriMetric = metrics.getCreateMultipartKeyFailure();
    try {
      keyEndpoint.put(bucketName, keyName, CONTENT.length(),
          1, "randomId", null);
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getCreateMultipartKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsSuccess() throws Exception {

    long oriMetric = metrics.getListPartsSuccess();
    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    // Listing out the parts by providing the uploadID
    keyEndpoint.get(bucketName, keyName,
        uploadID, 3, null, null);
    long curMetric = metrics.getListPartsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsFailure() throws Exception {

    long oriMetric = metrics.getListPartsFailure();
    try {
      // Listing out the parts by providing the uploadID after aborting
      keyEndpoint.get(bucketName, keyName,
          "wrong_id", 3, null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
    }
    long curMetric = metrics.getListPartsFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCopyObject() throws Exception {

    String destBucket = "b2";
    String destKey = "key2";

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(destBucket);


    // Test for Success of CopyObjectSuccess Metric
    long oriMetric = metrics.getCopyObjectSuccess();
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    keyEndpoint.put(bucketName, keyName,
        CONTENT.length(), 1, null, body);

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        bucketName + "/" + urlEncode(keyName));

    keyEndpoint.put(destBucket, destKey, CONTENT.length(), 1,
        null, body);
    long curMetric = metrics.getCopyObjectSuccess();
    assertEquals(1L, curMetric - oriMetric);

    // Test for Failure of CopyObjectFailure Metric
    oriMetric = metrics.getCopyObjectFailure();
    // source and dest same
    try {
      when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");
      keyEndpoint.put(bucketName, keyName, CONTENT.length(), 1, null, body);
      fail("Test for CopyObjectMetric failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getErrorMessage().contains("This copy request is " +
          "illegal"));
    }
    curMetric = metrics.getCopyObjectFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    for (String key : keys) {
      bucket.createKey(key, 0).close();
    }
    return clientStub;
  }

  private String initiateMultipartUpload(String bktName, String key)
      throws IOException,
      OS3Exception {
    // Initiate the Upload
    Response response =
        keyEndpoint.initializeMultipartUpload(bktName, key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    if (response.getStatus() == 200) {
      // Fetch the Upload-Id
      String uploadID = multipartUploadInitiateResponse.getUploadID();
      return uploadID;
    }
    return "Invalid-Id";
  }
}
