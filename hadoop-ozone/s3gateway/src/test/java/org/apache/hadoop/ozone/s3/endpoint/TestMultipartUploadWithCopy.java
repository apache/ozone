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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;

import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Class to test Multipart upload where parts are created with copy header.
 */

public class TestMultipartUploadWithCopy {

  private static final ObjectEndpoint REST = new ObjectEndpoint();

  private static final String KEY = "key2";
  private static final String EXISTING_KEY = "key1";
  private static final String EXISTING_KEY_CONTENT = "testkey";
  private static final OzoneClient CLIENT = new OzoneClientStub();
  private static final long DELAY_MS = 2000;
  private static long sourceKeyLastModificationTime;
  private static String beforeTimeStr;
  private static String afterTimeStr;
  private static String futureTimeStr;
  private static final String UNPARSABLE_TIME_STR = "Unparsable time string";
  private static final String ERROR_CODE = S3ErrorTable.PRECOND_FAILED.getCode();
  @BeforeClass
  public static void setUp() throws Exception {
    CLIENT.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    OzoneBucket bucket =
        CLIENT.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET);

    byte[] keyContent = EXISTING_KEY_CONTENT.getBytes(UTF_8);
    try (OutputStream stream = bucket
        .createKey(EXISTING_KEY, keyContent.length, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>())) {
      stream.write(keyContent);
    }

    sourceKeyLastModificationTime = CLIENT.getObjectStore()
        .getS3Bucket(OzoneConsts.S3_BUCKET)
        .getKey(EXISTING_KEY)
        .getModificationTime().toEpochMilli();
    beforeTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime - 1000);
    afterTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime + DELAY_MS);
    futureTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime +
            1000 * 60 * 24);

    // Make sure DELAY_MS has passed, otherwise
    //  afterTimeStr will be in the future
    //  and thus invalid
    long currentTime = new Date().getTime();
    long sleepMs = sourceKeyLastModificationTime + DELAY_MS - currentTime;
    if (sleepMs > 0) {
      Thread.sleep(sleepMs);
    }
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(CLIENT);
  }

  @Test
  public void testMultipart() throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(KEY);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";

    Part part1 = uploadPart(KEY, uploadID, 1, content);
    partsList.add(part1);

    Part part2 =
        uploadPartWithCopy(KEY, uploadID, 2,
            OzoneConsts.S3_BUCKET + "/" + EXISTING_KEY, null);
    partsList.add(part2);

    Part part3 =
        uploadPartWithCopy(KEY, uploadID, 3,
            OzoneConsts.S3_BUCKET + "/" + EXISTING_KEY, "bytes=0-3");
    partsList.add(part3);

    Part part4 =
        uploadPartWithCopy(KEY, uploadID, 3,
            OzoneConsts.S3_BUCKET + "/" + EXISTING_KEY, "bytes=0-3",
            beforeTimeStr,
            afterTimeStr
            );
    partsList.add(part4);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);

    completeMultipartUpload(KEY, completeMultipartUploadRequest,
        uploadID);

    OzoneBucket bucket =
        CLIENT.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET);
    try (InputStream is = bucket.readKey(KEY)) {
      String keyContent = new Scanner(is, UTF_8.name())
          .useDelimiter("\\A").next();
      Assert.assertEquals(
          content + EXISTING_KEY_CONTENT + EXISTING_KEY_CONTENT.substring(0, 4),
          keyContent);
    }
  }


  // CopyIfTimestampTestCase captures all the possibilities for the time stamps 
  // that can be passed into the multipart copy with copy-if flags for 
  // timestamps. Only some of the cases are valid others should raise an 
  // exception.
  // Time stamps can be, 
  // 1. after the timestamp on the object but still a valid time stamp 
  // (in regard to wall clock time on server)
  // 2. before the timestamp on the object
  // 3. In the Future beyond the wall clock time on the server
  // 4. Null
  // 5. Unparsable 
  //
  public enum CopyIfTimestampTestCase {
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_AFTER_TS(
        afterTimeStr, afterTimeStr, ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_BEFORE_TS(
        afterTimeStr, beforeTimeStr, ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_NULL(
        afterTimeStr, null, ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_FUTURE(
        afterTimeStr, futureTimeStr, ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        afterTimeStr, UNPARSABLE_TIME_STR, ERROR_CODE),

    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_AFTER_TS(
        beforeTimeStr, afterTimeStr, null),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        beforeTimeStr, beforeTimeStr, ERROR_CODE),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_NULL(
        beforeTimeStr, null, null),
    MODIFIED_SINCE_BEFORE_TS_UNMOFIFIED_SINCE_FUTURE(
        beforeTimeStr,futureTimeStr, null),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        beforeTimeStr, UNPARSABLE_TIME_STR, null),

    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_AFTER_TS(
        null, afterTimeStr, null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_BEFORE_TS(
        null, beforeTimeStr, ERROR_CODE),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_NULL_TS(
        null, null, null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_FUTURE_TS(
        null, futureTimeStr, null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        null, UNPARSABLE_TIME_STR, null),

    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_AFTER_TS(
        UNPARSABLE_TIME_STR, afterTimeStr, null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        UNPARSABLE_TIME_STR, beforeTimeStr, ERROR_CODE),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_NULL_TS(
        UNPARSABLE_TIME_STR, null, null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_FUTURE_TS(
        UNPARSABLE_TIME_STR, futureTimeStr, null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        UNPARSABLE_TIME_STR, UNPARSABLE_TIME_STR, null),

    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_AFTER_TS(
        futureTimeStr, afterTimeStr, null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        futureTimeStr, beforeTimeStr, ERROR_CODE),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_NULL_TS(
        futureTimeStr, null, null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_FUTURE_TS(
        futureTimeStr, futureTimeStr, null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        futureTimeStr, UNPARSABLE_TIME_STR, null);
    private final String modifiedTimestamp;
    private final String unmodifiedTimestamp;
    private final String errorCode;

    CopyIfTimestampTestCase(String modifiedTimestamp, String unmodifiedTimestamp, String errorCode) {
      this.modifiedTimestamp = modifiedTimestamp;
      this.unmodifiedTimestamp = unmodifiedTimestamp;
      this.errorCode = errorCode;
    }

    @Override
    public String toString() {
      return this.name() +
          " Modified:" + this.modifiedTimestamp
          + " Unmodified:" + this.unmodifiedTimestamp
          + " ErrorCode:" + this.errorCode;
    }
  }
  @Test
  public void testMultipartTSHeaders() throws Exception {
    for (CopyIfTimestampTestCase t : CopyIfTimestampTestCase.values() ) {
      try {
        uploadPartWithCopy(t.modifiedTimestamp, t.unmodifiedTimestamp);
        if (t.errorCode != null) {
          fail("Fail test:" + t);
        }
      } catch (OS3Exception ex) {
        if (t.errorCode == null) {
          fail("Failed test:" + t );
        }
      }
    }
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    setHeaders();
    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    return uploadID;

  }

  private Part uploadPart(String key, String uploadID, int partNumber, String
      content) throws IOException, OS3Exception {
    setHeaders();
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = REST.put(OzoneConsts.S3_BUCKET, key, content.length(),
        partNumber, uploadID, body);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString("ETag"));
    Part part = new Part();
    part.seteTag(response.getHeaderString("ETag"));
    part.setPartNumber(partNumber);

    return part;
  }

  private Part uploadPartWithCopy(String key, String uploadID, int partNumber,
      String keyOrigin, String range) throws IOException, OS3Exception {
    return uploadPartWithCopy(key, uploadID, partNumber, keyOrigin,
        range, null, null);
  }

  private Part uploadPartWithCopy(String ifModifiedSinceStr,
      String ifUnmodifiedSinceStr) throws IOException, OS3Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(KEY);

    return uploadPartWithCopy(KEY, uploadID, 1,
      OzoneConsts.S3_BUCKET + "/" + EXISTING_KEY, "bytes=0-3",
      ifModifiedSinceStr, ifUnmodifiedSinceStr);
  }

  private Part uploadPartWithCopy(String key, String uploadID, int partNumber,
      String keyOrigin, String range, String ifModifiedSinceStr,
      String ifUnmodifiedSinceStr) throws IOException, OS3Exception {
    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders.put(COPY_SOURCE_HEADER, keyOrigin);
    if (range != null) {
      additionalHeaders.put(COPY_SOURCE_HEADER_RANGE, range);
    }
    if (ifModifiedSinceStr != null) {
      additionalHeaders.put(COPY_SOURCE_IF_MODIFIED_SINCE, ifModifiedSinceStr);
    }
    if (ifUnmodifiedSinceStr != null) {
      additionalHeaders.put(COPY_SOURCE_IF_UNMODIFIED_SINCE,
          ifUnmodifiedSinceStr);
    }
    setHeaders(additionalHeaders);

    ByteArrayInputStream body = new ByteArrayInputStream("".getBytes(UTF_8));
    Response response = REST.put(OzoneConsts.S3_BUCKET, key, 0, partNumber,
        uploadID, body);
    assertEquals(200, response.getStatus());

    CopyPartResult result = (CopyPartResult) response.getEntity();
    assertNotNull(result.getETag());
    assertNotNull(result.getLastModified());
    Part part = new Part();
    part.seteTag(result.getETag());
    part.setPartNumber(partNumber);

    return part;
  }

  private void completeMultipartUpload(String key,
      CompleteMultipartUploadRequest completeMultipartUploadRequest,
      String uploadID) throws IOException, OS3Exception {
    setHeaders();
    Response response = REST.completeMultipartUpload(OzoneConsts.S3_BUCKET, key,
        uploadID, completeMultipartUploadRequest);

    assertEquals(200, response.getStatus());

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();

    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getBucket());
    assertEquals(KEY, completeMultipartUploadResponse.getKey());
    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getLocation());
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  private void setHeaders(Map<String, String> additionalHeaders) {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    additionalHeaders
        .forEach((k, v) -> when(headers.getHeaderString(k)).thenReturn(v));
    REST.setHeaders(headers);
  }

  private void setHeaders() {
    setHeaders(new HashMap<>());
  }

}
