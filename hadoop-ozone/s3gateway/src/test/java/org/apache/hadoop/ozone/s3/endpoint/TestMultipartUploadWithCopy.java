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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Class to test Multipart upload where parts are created with copy header.
 */

public class TestMultipartUploadWithCopy {

  private static final String KEY = "key2";
  private static final String EXISTING_KEY = "key1";
  private static final String EXISTING_KEY_CONTENT = "testkey";
  private static final long DELAY_MS = 2000;
  private static String beforeSourceKeyModificationTimeStr;
  private static String afterSourceKeyModificationTimeStr;
  private static String futureTimeStr;
  private static final String UNPARSABLE_TIME_STR = "Unparsable time string";
  private static final String ERROR_CODE =
      S3ErrorTable.PRECOND_FAILED.getCode();

  private static ObjectEndpoint endpoint;
  private static OzoneClient client;

  @BeforeAll
  public static void setUp() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET);

    byte[] keyContent = EXISTING_KEY_CONTENT.getBytes(UTF_8);
    try (OutputStream stream = bucket
        .createKey(EXISTING_KEY, keyContent.length,
            ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
            ReplicationFactor.THREE),
            new HashMap<String, String>() {{
              put(OzoneConsts.ETAG, DigestUtils.md5Hex(EXISTING_KEY_CONTENT));
            }}
        )) {
      stream.write(keyContent);
    }

    long sourceKeyLastModificationTime = client.getObjectStore()
                                             .getS3Bucket(OzoneConsts.S3_BUCKET)
                                             .getKey(EXISTING_KEY)
                                             .getModificationTime().toEpochMilli();
    beforeSourceKeyModificationTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime - 1000);
    afterSourceKeyModificationTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime + DELAY_MS);
    futureTimeStr =
        OzoneUtils.formatTime(sourceKeyLastModificationTime +
            1000 * 60 * 24);

    // Make sure DELAY_MS has passed, otherwise
    //  afterSourceKeyModificationTimeStr will be in the future
    //  and thus invalid
    long currentTime = System.currentTimeMillis();
    long sleepMs = sourceKeyLastModificationTime + DELAY_MS - currentTime;
    if (sleepMs > 0) {
      Thread.sleep(sleepMs);
    }
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
  }

  @Test
  public void testMultipart() throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, KEY);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";

    setHeaders();
    Part part1 = uploadPart(endpoint, OzoneConsts.S3_BUCKET, KEY, 1, uploadID, content);
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
            beforeSourceKeyModificationTimeStr,
            afterSourceKeyModificationTimeStr
            );
    partsList.add(part4);

    setHeaders();
    completeMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, TestMultipartUploadWithCopy.KEY, uploadID, partsList);

    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET);
    try (InputStream is = bucket.readKey(KEY)) {
      String keyContent = new Scanner(is, UTF_8.name())
          .useDelimiter("\\A").next();
      assertEquals(
          content + EXISTING_KEY_CONTENT + EXISTING_KEY_CONTENT.substring(0, 4),
          keyContent);
    }
  }

  /**
  * CopyIfTimestampTestCase captures all the possibilities for the time stamps
  * that can be passed into the multipart copy with copy-if flags for
  * timestamps. Only some of the cases are valid others should raise an
  * exception.
  * Time stamps can be,
  * 1. after the timestamp on the object but still a valid time stamp
  * (in regard to wall clock time on server)
  * 2. before the timestamp on the object
  * 3. In the Future beyond the wall clock time on the server
  * 4. Null
  * 5. Unparsable
  */
  public enum CopyIfTimestampTestCase {
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_AFTER_TS(
        afterSourceKeyModificationTimeStr, afterSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_BEFORE_TS(
        afterSourceKeyModificationTimeStr, beforeSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_NULL(
        afterSourceKeyModificationTimeStr, null,
        ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_FUTURE(
        afterSourceKeyModificationTimeStr, futureTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_AFTER_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        afterSourceKeyModificationTimeStr, UNPARSABLE_TIME_STR,
        ERROR_CODE),

    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_AFTER_TS(
        beforeSourceKeyModificationTimeStr, afterSourceKeyModificationTimeStr,
        null),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        beforeSourceKeyModificationTimeStr, beforeSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_NULL(
        beforeSourceKeyModificationTimeStr, null,
        null),
    MODIFIED_SINCE_BEFORE_TS_UNMOFIFIED_SINCE_FUTURE(
        beforeSourceKeyModificationTimeStr, futureTimeStr,
        null),
    MODIFIED_SINCE_BEFORE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        beforeSourceKeyModificationTimeStr, UNPARSABLE_TIME_STR,
        null),

    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_AFTER_TS(
        null, afterSourceKeyModificationTimeStr,
        null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_BEFORE_TS(
        null, beforeSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_NULL_TS(
        null, null,
        null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_FUTURE_TS(
        null, futureTimeStr,
        null),
    MODIFIED_SINCE_NULL_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        null, UNPARSABLE_TIME_STR,
        null),

    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_AFTER_TS(
        UNPARSABLE_TIME_STR, afterSourceKeyModificationTimeStr,
        null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        UNPARSABLE_TIME_STR, beforeSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_NULL_TS(
        UNPARSABLE_TIME_STR, null,
        null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_FUTURE_TS(
        UNPARSABLE_TIME_STR, futureTimeStr,
        null),
    MODIFIED_SINCE_UNPARSABLE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        UNPARSABLE_TIME_STR, UNPARSABLE_TIME_STR,
        null),

    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_AFTER_TS(
        futureTimeStr, afterSourceKeyModificationTimeStr,
        null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_BEFORE_TS(
        futureTimeStr, beforeSourceKeyModificationTimeStr,
        ERROR_CODE),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_NULL_TS(
        futureTimeStr, null,
        null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_FUTURE_TS(
        futureTimeStr, futureTimeStr,
        null),
    MODIFIED_SINCE_FUTURE_TS_UNMODIFIED_SINCE_UNPARSABLE_TS(
        futureTimeStr, UNPARSABLE_TIME_STR,
        null);
    
    private final String modifiedTimestamp;
    private final String unmodifiedTimestamp;
    private final String errorCode;

    CopyIfTimestampTestCase(String modifiedTimestamp,
        String unmodifiedTimestamp, String errorCode) {
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
    for (CopyIfTimestampTestCase t : CopyIfTimestampTestCase.values()) {
      try {
        uploadPartWithCopy(t.modifiedTimestamp, t.unmodifiedTimestamp);
        if (t.errorCode != null) {
          fail("Fail test:" + t);
        }
      } catch (OS3Exception ex) {
        if ((t.errorCode == null) || (!ex.getCode().equals(ERROR_CODE))) {
          fail("Failed test:" + t);
        }
      }
    }
  }

  private Part uploadPartWithCopy(String key, String uploadID, int partNumber,
      String keyOrigin, String range) throws IOException, OS3Exception {
    return uploadPartWithCopy(key, uploadID, partNumber, keyOrigin,
        range, null, null);
  }

  private Part uploadPartWithCopy(String ifModifiedSinceStr,
      String ifUnmodifiedSinceStr) throws IOException, OS3Exception {
    // Initiate multipart upload
    setHeaders();
    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, KEY);

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

    try (Response response = put(endpoint, OzoneConsts.S3_BUCKET, key, partNumber, uploadID, "")) {
      assertEquals(200, response.getStatus());

      CopyPartResult result = (CopyPartResult) response.getEntity();
      assertNotNull(result.getETag());
      assertNotNull(result.getLastModified());
      Part part = new Part();
      part.setETag(result.getETag());
      part.setPartNumber(partNumber);

      return part;
    }
  }

  @Test
  public void testUploadWithRangeCopyContentLength()
      throws IOException, OS3Exception {
    // The contentLength specified when creating the Key should be the same as
    // the Content-Length, the key Commit will compare the Content-Length with
    // the actual length of the data written.

    String uploadID = initiateMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, KEY);
    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders.put(COPY_SOURCE_HEADER,
        OzoneConsts.S3_BUCKET + "/" + EXISTING_KEY);
    additionalHeaders.put(COPY_SOURCE_HEADER_RANGE, "bytes=0-3");
    setHeaders(additionalHeaders);
    assertSucceeds(() -> put(endpoint, OzoneConsts.S3_BUCKET, KEY, 1, uploadID, ""));
    OzoneMultipartUploadPartListParts parts =
        client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET)
        .listParts(KEY, uploadID, 0, 100);
    assertEquals(1, parts.getPartInfoList().size());
    assertEquals(4, parts.getPartInfoList().get(0).getSize());
  }

  private void setHeaders(Map<String, String> additionalHeaders) {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    additionalHeaders
        .forEach((k, v) -> when(headers.getHeaderString(k)).thenReturn(v));
    endpoint.setHeaders(headers);
  }

  private void setHeaders() {
    setHeaders(new HashMap<>());
  }

}
