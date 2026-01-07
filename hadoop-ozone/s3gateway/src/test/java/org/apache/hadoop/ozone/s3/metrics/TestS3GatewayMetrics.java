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

package org.apache.hadoop.ozone.s3.metrics;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.BucketEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest;
import org.apache.hadoop.ozone.s3.endpoint.EndpointBuilder;
import org.apache.hadoop.ozone.s3.endpoint.MultipartUploadInitiateResponse;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.RootEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.TestBucketAcl;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @BeforeEach
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);
    bucket.createKey("file1", 0).close();

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();

    rootEndpoint = EndpointBuilder.newRootEndpointBuilder()
        .setClient(clientStub)
        .build();

    keyEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();

    metrics = bucketEndpoint.getMetrics();
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

    bucketEndpoint.get(bucketName).getEntity();

    long curMetric = metrics.getGetBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketFailure() throws Exception {
    long oriMetric = metrics.getGetBucketFailure();

    // Searching for a bucket that does not exist
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get("newBucket"));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
        e.getErrorMessage());
    long curMetric = metrics.getGetBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketSuccess() throws Exception {

    long oriMetric = metrics.getCreateBucketSuccess();
    assertDoesNotThrow(() -> bucketEndpoint.put("newBucket", null));
    long curMetric = metrics.getCreateBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketFailure() throws Exception {
    long oriMetric = metrics.getCreateBucketFailure();

    // Creating an error by trying to create a bucket that already exists
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.put(
        bucketName, null));
    assertEquals(HTTP_CONFLICT, e.getHttpCode());
    assertEquals(BUCKET_ALREADY_EXISTS.getCode(), e.getCode());

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

    // Deleting a bucket that does not exist will result in delete failure
    OS3Exception e = assertThrows(OS3Exception.class, () ->
        bucketEndpoint.delete(bucketName));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
        e.getErrorMessage());

    long curMetric = metrics.getDeleteBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclSuccess() throws Exception {
    long oriMetric = metrics.getGetAclSuccess();

    bucketEndpoint.queryParamsForTest().set(QueryParams.ACL, ACL_MARKER);
    Response response = bucketEndpoint.get(bucketName);
    long curMetric = metrics.getGetAclSuccess();
    assertEquals(HTTP_OK, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclFailure() throws Exception {
    long oriMetric = metrics.getGetAclFailure();

    bucketEndpoint.queryParamsForTest().set(QueryParams.ACL, ACL_MARKER);
    // Failing the getACL endpoint by applying ACL on a non-Existent Bucket
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get("random_bucket"));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
        e.getErrorMessage());
    long curMetric = metrics.getGetAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclSuccess() throws Exception {
    long oriMetric = metrics.getPutAclSuccess();

    clientStub.getObjectStore().createS3Bucket("b1");
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    bucketEndpoint.queryParamsForTest().set(QueryParams.ACL, ACL_MARKER);
    bucketEndpoint.put("b1", inputBody);
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
    bucketEndpoint.queryParamsForTest().set(QueryParams.ACL, ACL_MARKER);
    try {
      assertThrows(OS3Exception.class, () -> bucketEndpoint.put("unknown_bucket", inputBody));
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
        .length(), 1, null, null, null, body);
    body.close();
    long curMetric = metrics.getCreateKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateKeyFailure() throws Exception {
    long oriMetric = metrics.getCreateKeyFailure();

    // Create the file in a bucket that does not exist
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.put(
        "unknownBucket", keyName, CONTENT.length(), 1, null, null,
        null, null));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
    long curMetric = metrics.getCreateKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteKeySuccess() throws Exception {
    long oriMetric = metrics.getDeleteKeySuccess();

    bucket.createKey(keyName, 0).close();
    keyEndpoint.delete(bucketName, keyName, null, null);
    long curMetric = metrics.getDeleteKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteKeyFailure() throws Exception {
    long oriMetric = metrics.getDeleteKeyFailure();
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.delete(
        "unknownBucket", keyName, null, null));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
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
        .length(), 1, null, null, null, body);
    // GET the key from the bucket
    Response response = keyEndpoint.get(bucketName, keyName, 0, null, 0, null, null);
    StreamingOutput stream = (StreamingOutput) response.getEntity();
    stream.write(new ByteArrayOutputStream());
    long curMetric = metrics.getGetKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetKeyFailure() throws Exception {
    long oriMetric = metrics.getGetKeyFailure();

    // Fetching a non-existent key
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.get(
        bucketName, "unknownKey", 0, null, 0, null, null));
    assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), e.getCode());
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
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint
        .initializeMultipartUpload("unknownBucket", keyName));
    assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), e.getCode());
    long curMetric = metrics.getInitMultiPartUploadFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadSuccess() throws Exception {

    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    long oriMetric = metrics.getAbortMultiPartUploadSuccess();

    // Abort the Upload Successfully by deleting the key using the Upload-Id
    keyEndpoint.delete(bucketName, keyName, uploadID, null);

    long curMetric = metrics.getAbortMultiPartUploadSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testAbortMultiPartUploadFailure() throws Exception {
    long oriMetric = metrics.getAbortMultiPartUploadFailure();

    // Fail the Abort Method by providing wrong uploadID
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.delete(
        bucketName, keyName, "wrongId", null));
    assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), e.getCode());
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
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint
        .completeMultipartUpload(bucketName, "key2", "random",
            completeMultipartUploadRequestNew));
    assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), e.getCode());
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
        1, uploadID, null, null, body);
    long curMetric = metrics.getCreateMultipartKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateMultipartKeyFailure() throws Exception {
    long oriMetric = metrics.getCreateMultipartKeyFailure();
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.put(
        bucketName, keyName, CONTENT.length(), 1, "randomId", null, null, null));
    assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), e.getCode());
    long curMetric = metrics.getCreateMultipartKeyFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsSuccess() throws Exception {

    long oriMetric = metrics.getListPartsSuccess();
    // Initiate the Upload and fetch the upload ID
    String uploadID = initiateMultipartUpload(bucketName, keyName);

    // Listing out the parts by providing the uploadID
    keyEndpoint.get(bucketName, keyName, 0,
        uploadID, 3, null, null);
    long curMetric = metrics.getListPartsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListPartsFailure() throws Exception {

    long oriMetric = metrics.getListPartsFailure();
    // Listing out the parts by providing the uploadID after aborting
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.get(
        bucketName, keyName, 0, "wrong_id", 3, null, null));
    assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), e.getCode());
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
        CONTENT.length(), 1, null, null, null, body);

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        bucketName + "/" + urlEncode(keyName));

    keyEndpoint.put(destBucket, destKey, CONTENT.length(), 1,
        null, null, null, body);
    long curMetric = metrics.getCopyObjectSuccess();
    assertEquals(1L, curMetric - oriMetric);

    // Test for Failure of CopyObjectFailure Metric
    oriMetric = metrics.getCopyObjectFailure();
    // source and dest same
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");
    OS3Exception e = assertThrows(OS3Exception.class, () -> keyEndpoint.put(
        bucketName, keyName, CONTENT.length(), 1, null, null, null, body),
        "Test for CopyObjectMetric failed");
    assertThat(e.getErrorMessage()).contains("This copy request is illegal");
    curMetric = metrics.getCopyObjectFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutObjectTaggingSuccess() throws Exception {
    long oriMetric = metrics.getPutObjectTaggingSuccess();

    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Create the file
    keyEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, null, null, body);
    body.close();

    // Put object tagging
    keyEndpoint.put(bucketName, keyName, 0, 1, null, "", null, getPutTaggingBody());

    long curMetric = metrics.getPutObjectTaggingSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutObjectTaggingFailure() throws Exception {
    long oriMetric = metrics.getPutObjectTaggingFailure();

    // Put object tagging for nonexistent key
    OS3Exception ex = assertThrows(OS3Exception.class, () ->
        keyEndpoint.put(bucketName, "nonexistent", 0, 1, null, "",
            null, getPutTaggingBody())
    );
    assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());

    long curMetric = metrics.getPutObjectTaggingFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetObjectTaggingSuccess() throws Exception {
    long oriMetric = metrics.getGetObjectTaggingSuccess();

    // Create the file
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, null, null, body);
    body.close();

    // Put object tagging
    keyEndpoint.put(bucketName, keyName, 0, 1, null, "", null, getPutTaggingBody());

    // Get object tagging
    keyEndpoint.get(bucketName, keyName, 0,
        null, 0,  null, "");

    long curMetric = metrics.getGetObjectTaggingSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetObjectTaggingFailure() throws Exception {
    long oriMetric = metrics.getGetObjectTaggingFailure();

    // Get object tagging for nonexistent key
    OS3Exception ex = assertThrows(OS3Exception.class, () ->
        keyEndpoint.get(bucketName, "nonexistent", 0, null,
            0, null, ""));
    assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());
    long curMetric = metrics.getGetObjectTaggingFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteObjectTaggingSuccess() throws Exception {
    long oriMetric = metrics.getDeleteObjectTaggingSuccess();

    // Create the file
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    keyEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, null, null, body);
    body.close();

    // Put object tagging
    keyEndpoint.put(bucketName, keyName, 0, 1, null, "", null, getPutTaggingBody());

    // Delete object tagging
    keyEndpoint.delete(bucketName, keyName, null, "");

    long curMetric = metrics.getDeleteObjectTaggingSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteObjectTaggingFailure() throws Exception {
    long oriMetric = metrics.getDeleteObjectTaggingFailure();

    // Delete object tagging for nonexistent key
    OS3Exception ex = assertThrows(OS3Exception.class, () ->
        keyEndpoint.delete(bucketName, "nonexistent", null, ""));
    assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());
    long curMetric = metrics.getDeleteObjectTaggingFailure();
    assertEquals(1L, curMetric - oriMetric);
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

  private static InputStream getPutTaggingBody() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>tag1</Key>" +
            "         <Value>val1</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }
}
