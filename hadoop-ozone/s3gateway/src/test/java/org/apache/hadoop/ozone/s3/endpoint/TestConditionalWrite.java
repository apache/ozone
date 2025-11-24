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
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test conditional writes (If-Match, If-None-Match) for PutObject.
 */
class TestConditionalWrite {
  private static final String BUCKET_NAME = "test-bucket";
  private static final String KEY_NAME = "test-key";
  private static final String CONTENT = "test content";

  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;
  private HttpHeaders headers;
  private OzoneBucket bucket;

  @BeforeEach
  void setup() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();

    // Create client stub and object store stub
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");

    // Create ObjectEndpoint and set client to OzoneClientStub
    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setConfig(config)
        .setHeaders(headers)
        .build();

    objectEndpoint = spy(objectEndpoint);
  }

  /**
   * Test If-None-Match: * succeeds when key doesn't exist.
   */
  @Test
  void testIfNoneMatchSuccessWhenKeyNotExists() throws Exception {
    InputStream inputStream = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    Response response = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        CONTENT.length(),
        "*",  // If-None-Match: *
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream
    );

    assertEquals(HttpStatus.SC_OK, response.getStatus());
    assertNotNull(response.getHeaderString("ETag"));

    // Verify key was created
    OzoneKeyDetails keyDetails = bucket.getKey(KEY_NAME);
    assertNotNull(keyDetails);
  }

  /**
   * Test If-None-Match: * fails when key already exists.
   */
  @Test
  void testIfNoneMatchFailsWhenKeyExists() throws Exception {
    // First, create the key normally
    InputStream inputStream1 = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    Response response1 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        CONTENT.length(),
        null, // If-None-Match
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream1
    );
    assertEquals(HttpStatus.SC_OK, response1.getStatus());

    // Second, try to create the same key with If-None-Match: *
    InputStream inputStream2 = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          CONTENT.length(),
          "*",  // If-None-Match: *
          null, // If-Match
          0,    // partNumber
          "",   // uploadID
          null, // taggingMarker
          null, // aclMarker
          inputStream2
      );
    });

    assertEquals(HttpStatus.SC_PRECONDITION_FAILED, exception.getHttpCode());
    assertEquals(S3ErrorTable.PRECOND_FAILED.getCode(), exception.getCode());
  }

  /**
   * Test If-Match succeeds when ETag matches.
   */
  @Test
  void testIfMatchSuccessWhenETagMatches() throws Exception {
    // First, create the key normally
    InputStream inputStream1 = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    Response response1 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        CONTENT.length(),
        null, // If-None-Match
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream1
    );
    assertEquals(HttpStatus.SC_OK, response1.getStatus());
    String etag = stripQuotes(response1.getHeaderString("ETag"));

    // Second, update the key with If-Match using the correct ETag
    String newContent = "updated content";
    InputStream inputStream2 = new ByteArrayInputStream(newContent.getBytes(UTF_8));
    Response response2 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        newContent.length(),
        null,  // If-None-Match
        "\"" + etag + "\"", // If-Match with quoted ETag
        0,     // partNumber
        "",    // uploadID
        null,  // taggingMarker
        null,  // aclMarker
        inputStream2
    );

    assertEquals(HttpStatus.SC_OK, response2.getStatus());
    assertNotNull(response2.getHeaderString("ETag"));
  }

  /**
   * Test If-Match fails when key doesn't exist.
   */
  @Test
  void testIfMatchFailsWhenKeyNotExists() throws Exception {
    String fakeEtag = "abc123";
    InputStream inputStream = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          CONTENT.length(),
          null,  // If-None-Match
          "\"" + fakeEtag + "\"", // If-Match
          0,     // partNumber
          "",    // uploadID
          null,  // taggingMarker
          null,  // aclMarker
          inputStream
      );
    });

    assertEquals(HttpStatus.SC_NOT_FOUND, exception.getHttpCode());
    assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), exception.getCode());
  }

  /**
   * Test If-Match fails when ETag doesn't match.
   */
  @Test
  void testIfMatchFailsWhenETagMismatch() throws Exception {
    // First, create the key normally
    InputStream inputStream1 = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    Response response1 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        CONTENT.length(),
        null, // If-None-Match
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream1
    );
    assertEquals(HttpStatus.SC_OK, response1.getStatus());

    // Second, try to update with wrong ETag
    String wrongEtag = "wrongetag123";
    String newContent = "updated content";
    InputStream inputStream2 = new ByteArrayInputStream(newContent.getBytes(UTF_8));

    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          newContent.length(),
          null,  // If-None-Match
          "\"" + wrongEtag + "\"", // If-Match with wrong ETag
          0,     // partNumber
          "",    // uploadID
          null,  // taggingMarker
          null,  // aclMarker
          inputStream2
      );
    });

    assertEquals(HttpStatus.SC_PRECONDITION_FAILED, exception.getHttpCode());
    assertEquals(S3ErrorTable.PRECOND_FAILED.getCode(), exception.getCode());
  }

  /**
   * Test that If-Match and If-None-Match are mutually exclusive.
   */
  @Test
  void testIfMatchAndIfNoneMatchMutuallyExclusive() throws Exception {
    InputStream inputStream = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          CONTENT.length(),
          "*",     // If-None-Match
          "\"abc\"", // If-Match
          0,       // partNumber
          "",      // uploadID
          null,    // taggingMarker
          null,    // aclMarker
          inputStream
      );
    });

    assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpCode());
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), exception.getCode());
  }

  /**
   * Test If-None-Match with invalid value (must be "*").
   */
  @Test
  void testIfNoneMatchInvalidValue() throws Exception {
    InputStream inputStream = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          CONTENT.length(),
          "\"etag\"", // If-None-Match with invalid value (must be "*")
          null,       // If-Match
          0,          // partNumber
          "",         // uploadID
          null,       // taggingMarker
          null,       // aclMarker
          inputStream
      );
    });

    assertEquals(HttpStatus.SC_BAD_REQUEST, exception.getHttpCode());
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), exception.getCode());
  }

  /**
   * Test generation mismatch detection - simulates concurrent modification.
   * This test verifies that when a key is modified between the If-Match check
   * and the actual rewrite, it returns 412 Precondition Failed.
   */
  @Test
  void testIfMatchGenerationMismatch() throws Exception {
    // First, create the key normally
    InputStream inputStream1 = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    Response response1 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        CONTENT.length(),
        null, // If-None-Match
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream1
    );
    assertEquals(HttpStatus.SC_OK, response1.getStatus());
    String etag = stripQuotes(response1.getHeaderString("ETag"));

    // Second, update the key normally (simulating concurrent modification)
    String intermediateContent = "intermediate update";
    InputStream inputStream2 = new ByteArrayInputStream(intermediateContent.getBytes(UTF_8));
    Response response2 = objectEndpoint.put(
        BUCKET_NAME,
        KEY_NAME,
        intermediateContent.length(),
        null, // If-None-Match
        null, // If-Match
        0,    // partNumber
        "",   // uploadID
        null, // taggingMarker
        null, // aclMarker
        inputStream2
    );
    assertEquals(HttpStatus.SC_OK, response2.getStatus());

    // Third, try to update using the old ETag (should fail with generation mismatch)
    String finalContent = "final update";
    InputStream inputStream3 = new ByteArrayInputStream(finalContent.getBytes(UTF_8));

    OS3Exception exception = assertThrows(OS3Exception.class, () -> {
      objectEndpoint.put(
          BUCKET_NAME,
          KEY_NAME,
          finalContent.length(),
          null,  // If-None-Match
          "\"" + etag + "\"", // If-Match with old ETag
          0,     // partNumber
          "",    // uploadID
          null,  // taggingMarker
          null,  // aclMarker
          inputStream3
      );
    });

    // Should return 412 Precondition Failed (not 404)
    assertEquals(HttpStatus.SC_PRECONDITION_FAILED, exception.getHttpCode());
    assertEquals(S3ErrorTable.PRECOND_FAILED.getCode(), exception.getCode());
  }
}

