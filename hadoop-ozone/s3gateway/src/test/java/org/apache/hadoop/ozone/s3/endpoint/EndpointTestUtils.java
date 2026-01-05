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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.http.HttpStatus;
import org.apache.ratis.util.function.CheckedSupplier;

/** Utilities for unit-testing S3 endpoints. */
public final class EndpointTestUtils {

  /** Get key tags. */
  public static Response getTagging(
      ObjectEndpoint subject,
      String bucket,
      String key
  ) throws IOException, OS3Exception {
    return subject.get(bucket, key, 0, null, 0, null, "");
  }

  /** Put without content. */
  public static Response putDir(
      ObjectEndpoint subject,
      String bucket,
      String key
  ) throws IOException, OS3Exception {
    return put(subject, bucket, key, 0, null, null);
  }

  /** Put with content. */
  public static Response put(
      ObjectEndpoint subject,
      String bucket,
      String key,
      String content
  ) throws IOException, OS3Exception {
    return put(subject, bucket, key, 0, null, content);
  }

  /** Add tagging on key. */
  public static Response putTagging(
      ObjectEndpoint subject,
      String bucket,
      String key,
      String content
  ) throws IOException, OS3Exception {
    if (content == null) {
      return subject.put(bucket, key, 0, 0, null, "", null, null);
    } else {
      final long length = content.length();
      try (ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes(UTF_8))) {
        return subject.put(bucket, key, length, 0, null, "", null, body);
      }
    }
  }

  /** Put with content, part number, upload ID. */
  public static Response put(
      ObjectEndpoint subject,
      String bucket,
      String key,
      int partNumber,
      String uploadID,
      String content
  ) throws IOException, OS3Exception {
    if (content == null) {
      return subject.put(bucket, key, 0, partNumber, uploadID, null, null, null);
    } else {
      final long length = content.length();
      try (ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes(UTF_8))) {
        return subject.put(bucket, key, length, partNumber, uploadID, null, null, body);
      }
    }
  }

  /** Delete key tags. */
  public static Response deleteTagging(
      ObjectEndpoint subject,
      String bucket,
      String key
  ) throws IOException, OS3Exception {
    return subject.delete(bucket, key, null, "");
  }

  /** Initiate multipart upload.
   * @return upload ID */
  public static String initiateMultipartUpload(ObjectEndpoint subject, String bucket, String key)
      throws IOException, OS3Exception {
    try (Response response = subject.initializeMultipartUpload(bucket, key)) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());
      MultipartUploadInitiateResponse entity = (MultipartUploadInitiateResponse) response.getEntity();
      String uploadID = entity.getUploadID();
      assertNotNull(uploadID, "uploadID == null");
      return uploadID;
    }
  }

  /** Upload part of multipart key.
   * @return Part to be used for completion request */
  public static CompleteMultipartUploadRequest.Part uploadPart(
      ObjectEndpoint subject,
      String bucket,
      String key,
      int partNumber,
      String uploadID,
      String content
  ) throws IOException, OS3Exception {
    CompleteMultipartUploadRequest.Part part = new CompleteMultipartUploadRequest.Part();

    try (Response response = put(subject, bucket, key, partNumber, uploadID, content)) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());
      String eTag = response.getHeaderString(OzoneConsts.ETAG);
      assertNotNull(eTag);
      part.setETag(eTag);
    }

    part.setPartNumber(partNumber);

    return part;
  }

  /** Complete multipart upload. */
  public static void completeMultipartUpload(
      ObjectEndpoint subject,
      String bucket,
      String key,
      String uploadID,
      List<CompleteMultipartUploadRequest.Part> parts
  ) throws IOException, OS3Exception {
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(parts);

    try (Response response = subject.completeMultipartUpload(bucket, key, uploadID, completeMultipartUploadRequest)) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());

      CompleteMultipartUploadResponse completeMultipartUploadResponse =
          (CompleteMultipartUploadResponse) response.getEntity();

      assertEquals(bucket, completeMultipartUploadResponse.getBucket());
      assertEquals(key, completeMultipartUploadResponse.getKey());
      assertEquals(bucket, completeMultipartUploadResponse.getLocation());
      assertNotNull(completeMultipartUploadResponse.getETag());
    }
  }

  /** Verify response is success for {@code request}. */
  public static <E extends Exception> void assertSucceeds(CheckedSupplier<Response, E> request) throws E {
    try (Response response = request.get()) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());
    }
  }

  /** Verify error response for {@code request} matching {@code expected} {@link OS3Exception}. */
  public static OS3Exception assertErrorResponse(OS3Exception expected, CheckedSupplier<Response, ?> request) {
    OS3Exception actual = assertThrows(OS3Exception.class, () -> request.get().close());
    assertEquals(expected.getCode(), actual.getCode());
    assertEquals(expected.getHttpCode(), actual.getHttpCode());
    return actual;
  }

  private EndpointTestUtils() {
    // no instances
  }
}
