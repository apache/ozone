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
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_TAG;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_KEY_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_NUM_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_VALUE_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Map;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Test put object.
 */
class TestObjectPut {
  private static final String CONTENT = "0123456789";
  private static final String FSO_BUCKET_NAME = "fso-bucket";
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key=value/1";
  private static final String DEST_BUCKET_NAME = "b2";
  private static final String DEST_KEY = "key=value/2";
  private static final String NO_SUCH_BUCKET = "nonexist";

  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;
  private HttpHeaders headers;
  private OzoneBucket bucket;
  private OzoneBucket fsoBucket;

  static Stream<Arguments> argumentsForPutObject() {
    ReplicationConfig ratis3 = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    ECReplicationConfig ec = new ECReplicationConfig("rs-3-2-1024K");
    return Stream.of(
        Arguments.of(0, ratis3),
        Arguments.of(10, ratis3),
        Arguments.of(0, ec),
        Arguments.of(10, ec)
    );
  }

  @BeforeEach
  void setup() throws IOException {
    OzoneConfiguration config = new OzoneConfiguration();

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME);
    clientStub.getObjectStore().createS3Bucket(DEST_BUCKET_NAME);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setConfig(config)
        .setHeaders(headers)
        .build();

    objectEndpoint = spy(objectEndpoint);

    String volumeName = config.get(OzoneConfigKeys.OZONE_S3_VOLUME_NAME,
        OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT);
    OzoneVolume volume = clientStub.getObjectStore().getVolume(volumeName);
    BucketArgs fsoBucketArgs = BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
    volume.createBucket(FSO_BUCKET_NAME, fsoBucketArgs);
    fsoBucket = volume.getBucket(FSO_BUCKET_NAME);
  }

  @ParameterizedTest
  @MethodSource("argumentsForPutObject")
  void testPutObject(int length, ReplicationConfig replication) throws IOException, OS3Exception {
    //GIVEN
    final String content = RandomStringUtils.secure().nextAlphanumeric(length);
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes(UTF_8));
    bucket.setReplicationConfig(replication);

    //WHEN
    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME, length, 1, body);

    //THEN
    assertEquals(200, response.getStatus());

    String keyContent;
    try (InputStream input = bucket.readKey(KEY_NAME)) {
      keyContent = IOUtils.toString(input, UTF_8);
    }
    assertEquals(content, keyContent);

    OzoneKeyDetails keyDetails = bucket.getKey(KEY_NAME);
    assertEquals(replication, keyDetails.getReplicationConfig());
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    assertThat(keyDetails.getTags()).isEmpty();
  }

  @Test
  void testPutObjectContentLength() throws IOException, OS3Exception {
    // The contentLength specified when creating the Key should be the same as
    // the Content-Length, the key Commit will compare the Content-Length with
    // the actual length of the data written.
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    long dataSize = CONTENT.length();

    objectEndpoint.put(BUCKET_NAME, KEY_NAME, dataSize, 0, body);
    assertEquals(dataSize, getKeyDataSize());
  }

  @Test
  void testPutObjectContentLengthForStreaming()
      throws IOException, OS3Exception {
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn("15");
    objectEndpoint.put(BUCKET_NAME, KEY_NAME, chunkedContent.length(), 0,
        new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));
    assertEquals(15, getKeyDataSize());
  }

  @Test
  public void testPutObjectWithTags() throws IOException, OS3Exception {
    HttpHeaders headersWithTags = Mockito.mock(HttpHeaders.class);
    when(headersWithTags.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithTags.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");

    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headersWithTags);

    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
        1, body);

    assertEquals(200, response.getStatus());

    OzoneKeyDetails keyDetails =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    Map<String, String> tags = keyDetails.getTags();
    assertEquals(2, tags.size());
    assertEquals("value1", tags.get("tag1"));
    assertEquals("value2", tags.get("tag2"));
  }

  @Test
  public void testPutObjectWithOnlyTagKey() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    HttpHeaders headerWithOnlyTagKey = Mockito.mock(HttpHeaders.class);
    when(headerWithOnlyTagKey.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    // Try to send with only the key (no value)
    when(headerWithOnlyTagKey.getHeaderString(TAG_HEADER)).thenReturn("tag1");
    objectEndpoint.setHeaders(headerWithOnlyTagKey);

    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
          1, body);
      fail("request with invalid query param should fail");
    } catch (OS3Exception ex) {
      assertEquals(INVALID_TAG.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("Some tag values are not specified");
      assertEquals(INVALID_TAG.getHttpCode(), ex.getHttpCode());
    }
  }

  @Test
  public void testPutObjectWithDuplicateTagKey() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    HttpHeaders headersWithDuplicateTagKey = Mockito.mock(HttpHeaders.class);
    when(headersWithDuplicateTagKey.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersWithDuplicateTagKey.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag1=value2");
    objectEndpoint.setHeaders(headersWithDuplicateTagKey);
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
          1, body);
      fail("request with duplicate tag key should fail");
    } catch (OS3Exception ex) {
      assertEquals(INVALID_TAG.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("There are tags with duplicate tag keys");
      assertEquals(INVALID_TAG.getHttpCode(), ex.getHttpCode());
    }
  }

  @Test
  public void testPutObjectWithLongTagKey() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    HttpHeaders headersWithLongTagKey = Mockito.mock(HttpHeaders.class);
    when(headersWithLongTagKey.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    String longTagKey = StringUtils.repeat('k', TAG_KEY_LENGTH_LIMIT + 1);
    when(headersWithLongTagKey.getHeaderString(TAG_HEADER)).thenReturn(longTagKey + "=value1");
    objectEndpoint.setHeaders(headersWithLongTagKey);
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
          1, body);
      fail("request with tag key exceeding the length limit should fail");
    } catch (OS3Exception ex) {
      assertEquals(INVALID_TAG.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("The tag key exceeds the maximum length");
      assertEquals(INVALID_TAG.getHttpCode(), ex.getHttpCode());
    }
  }

  @Test
  public void testPutObjectWithLongTagValue() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    HttpHeaders headersWithLongTagValue = Mockito.mock(HttpHeaders.class);
    when(headersWithLongTagValue.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    objectEndpoint.setHeaders(headersWithLongTagValue);
    String longTagValue = StringUtils.repeat('v', TAG_VALUE_LENGTH_LIMIT + 1);
    when(headersWithLongTagValue.getHeaderString(TAG_HEADER)).thenReturn("tag1=" + longTagValue);
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
          1, body);
      fail("request with tag value exceeding the length limit should fail");
    } catch (OS3Exception ex) {
      assertEquals(INVALID_TAG.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("The tag value exceeds the maximum length");
      assertEquals(INVALID_TAG.getHttpCode(), ex.getHttpCode());
    }
  }

  @Test
  public void testPutObjectWithTooManyTags() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    HttpHeaders headersWithTooManyTags = Mockito.mock(HttpHeaders.class);
    when(headersWithTooManyTags.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TAG_NUM_LIMIT + 1; i++) {
      sb.append(String.format("tag%d=value%d", i, i));
      if (i < TAG_NUM_LIMIT) {
        sb.append('&');
      }
    }
    when(headersWithTooManyTags.getHeaderString(TAG_HEADER)).thenReturn(sb.toString());
    objectEndpoint.setHeaders(headersWithTooManyTags);
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT.length(),
          1, body);
      fail("request with number of tags exceeding limit should fail");
    } catch (OS3Exception ex) {
      assertEquals(INVALID_TAG.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("exceeded the maximum number of tags");
      assertEquals(INVALID_TAG.getHttpCode(), ex.getHttpCode());
    }
  }

  private long getKeyDataSize() throws IOException {
    return clientStub.getObjectStore().getS3Bucket(BUCKET_NAME)
        .getKey(KEY_NAME).getDataSize();
  }

  @Test
  void testPutObjectWithSignedChunks() throws IOException, OS3Exception {
    //GIVEN
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn("15");

    //WHEN
    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME,
        chunkedContent.length(), 1,
        new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));

    //THEN
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME)
            .readKey(KEY_NAME);
    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    OzoneKeyDetails keyDetails = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);

    assertEquals(200, response.getStatus());
    assertEquals("1234567890abcde", keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
  }

  @Test
  public void testPutObjectMessageDigestResetDuringException() throws OS3Exception {
    MessageDigest messageDigest = mock(MessageDigest.class);
    try (MockedStatic<IOUtils> mocked = mockStatic(IOUtils.class)) {
      // For example, EOFException during put-object due to client cancelling the operation before it completes
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class), anyLong(),
              anyLong(), any(byte[].class)))
          .thenThrow(IOException.class);
      when(objectEndpoint.getMessageDigestInstance()).thenReturn(messageDigest);

      ByteArrayInputStream body =
          new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
      try {
        objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT
            .length(), 1, body);
        fail("Should throw IOException");
      } catch (IOException ignored) {
        // Verify that the message digest is reset so that the instance can be reused for the
        // next request in the same thread
        verify(messageDigest, times(1)).reset();
      }
    }
  }

  @Test
  void testCopyObject() throws IOException, OS3Exception {
    // Put object in to source bucket
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    // Add some custom metadata
    MultivaluedMap<String, String> metadataHeaders = new MultivaluedHashMap<>();
    metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-1", "custom-value-1");
    metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-2", "custom-value-2");
    when(headers.getRequestHeaders()).thenReturn(metadataHeaders);
    // Add COPY metadata directive (default)
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("COPY");

    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME,
        CONTENT.length(), 1, body);

    OzoneInputStream ozoneInputStream = clientStub.getObjectStore()
        .getS3Bucket(BUCKET_NAME)
        .readKey(KEY_NAME);

    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    OzoneKeyDetails keyDetails = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    assertThat(keyDetails.getMetadata().get("custom-key-1")).isEqualTo("custom-value-1");
    assertThat(keyDetails.getMetadata().get("custom-key-2")).isEqualTo("custom-value-2");

    String sourceETag = keyDetails.getMetadata().get(OzoneConsts.ETAG);

    // This will be ignored since the copy directive is COPY
    metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-3", "custom-value-3");

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    response = objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
        body);

    // Check destination key and response
    ozoneInputStream = clientStub.getObjectStore().getS3Bucket(DEST_BUCKET_NAME)
        .readKey(DEST_KEY);

    keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    OzoneKeyDetails sourceKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    OzoneKeyDetails destKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(DEST_BUCKET_NAME).getKey(DEST_KEY);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    // Source key eTag should remain unchanged and the dest key should have
    // the same Etag since the key content is the same
    assertEquals(sourceETag, sourceKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertEquals(sourceETag, destKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertThat(destKeyDetails.getMetadata().get("custom-key-1")).isEqualTo("custom-value-1");
    assertThat(destKeyDetails.getMetadata().get("custom-key-2")).isEqualTo("custom-value-2");
    assertThat(destKeyDetails.getMetadata().containsKey("custom-key-3")).isFalse();

    // Now use REPLACE metadata directive (default) and remove some custom metadata used in the source key
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("REPLACE");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-1");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-2");

    response = objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
        body);

    ozoneInputStream = clientStub.getObjectStore().getS3Bucket(DEST_BUCKET_NAME)
        .readKey(DEST_KEY);

    keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    sourceKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    destKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(DEST_BUCKET_NAME).getKey(DEST_KEY);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    // Source key eTag should remain unchanged and the dest key should have
    // the same Etag since the key content is the same
    assertEquals(sourceETag, sourceKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertEquals(sourceETag, destKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertThat(destKeyDetails.getMetadata().containsKey("custom-key-1")).isFalse();
    assertThat(destKeyDetails.getMetadata().containsKey("custom-key-2")).isFalse();
    assertThat(destKeyDetails.getMetadata().get("custom-key-3")).isEqualTo("custom-value-3");


    // wrong copy metadata directive
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("INVALID");
    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
            DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1, body),
        "test copy object failed");
    assertThat(e.getHttpCode()).isEqualTo(400);
    assertThat(e.getCode()).isEqualTo("InvalidArgument");
    assertThat(e.getErrorMessage()).contains("The metadata copy directive specified is invalid");

    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("COPY");

    // source and dest same
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
            BUCKET_NAME, KEY_NAME, CONTENT.length(), 1, body),
        "test copy object failed");
    assertThat(e.getErrorMessage()).contains("This copy request is illegal");

    // source bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        NO_SUCH_BUCKET + "/"  + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(DEST_BUCKET_NAME,
        DEST_KEY, CONTENT.length(), 1, body), "test copy object failed");
    assertThat(e.getCode()).contains("NoSuchBucket");

    // dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(NO_SUCH_BUCKET,
        DEST_KEY, CONTENT.length(), 1, body), "test copy object failed");
    assertThat(e.getCode()).contains("NoSuchBucket");

    //Both source and dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        NO_SUCH_BUCKET + "/" + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(NO_SUCH_BUCKET,
        DEST_KEY, CONTENT.length(), 1, body), "test copy object failed");
    assertThat(e.getCode()).contains("NoSuchBucket");

    // source key not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(NO_SUCH_BUCKET));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
        "nonexistent", KEY_NAME, CONTENT.length(), 1, body),
        "test copy object failed");
    assertThat(e.getCode()).contains("NoSuchBucket");
  }

  @Test
  public void testCopyObjectMessageDigestResetDuringException() throws IOException, OS3Exception {
    // Put object in to source bucket
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME,
        CONTENT.length(), 1, body);

    OzoneInputStream ozoneInputStream = clientStub.getObjectStore()
        .getS3Bucket(BUCKET_NAME)
        .readKey(KEY_NAME);

    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    OzoneKeyDetails keyDetails = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();

    MessageDigest messageDigest = mock(MessageDigest.class);
    try (MockedStatic<IOUtils> mocked = mockStatic(IOUtils.class)) {
      // Add the mocked methods only during the copy request
      when(objectEndpoint.getMessageDigestInstance()).thenReturn(messageDigest);
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class), anyLong(),
              anyLong(), any(byte[].class)))
          .thenThrow(IOException.class);

      // Add copy header, and then call put
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          BUCKET_NAME  + "/" + urlEncode(KEY_NAME));

      try {
        objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
            body);
        fail("Should throw IOException");
      } catch (IOException ignored) {
        // Verify that the message digest is reset so that the instance can be reused for the
        // next request in the same thread
        verify(messageDigest, times(1)).reset();
      }
    }
  }

  @Test
  public void testCopyObjectWithTags() throws IOException, OS3Exception {
    // Put object in to source bucket
    HttpHeaders headersForPut = Mockito.mock(HttpHeaders.class);
    when(headersForPut.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersForPut.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headersForPut);

    String sourceKeyName = "sourceKey";

    Response putResponse = objectEndpoint.put(BUCKET_NAME, sourceKeyName,
        CONTENT.length(), 1, body);
    OzoneKeyDetails keyDetails =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(sourceKeyName);

    assertEquals(200, putResponse.getStatus());
    Map<String, String> tags = keyDetails.getTags();
    assertEquals(2, tags.size());
    assertEquals("value1", tags.get("tag1"));
    assertEquals("value2", tags.get("tag2"));

    // Copy object without x-amz-tagging-directive (default to COPY)
    String destKey = "key=value/2";
    HttpHeaders headersForCopy = Mockito.mock(HttpHeaders.class);
    when(headersForCopy.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headersForCopy.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME  + "/" + urlEncode(sourceKeyName));

    objectEndpoint.setHeaders(headersForCopy);
    Response copyResponse = objectEndpoint.put(DEST_BUCKET_NAME, destKey, CONTENT.length(), 1, body);

    OzoneKeyDetails destKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(DEST_BUCKET_NAME).getKey(destKey);

    assertEquals(200, copyResponse.getStatus());
    Map<String, String> destKeyTags = destKeyDetails.getTags();

    // Since the default directive is COPY, it will copy the source key's tags
    // to the destination key
    assertEquals(2, destKeyTags.size());
    assertEquals("value1", destKeyTags.get("tag1"));
    assertEquals("value2", destKeyTags.get("tag2"));

    // Copy object with x-amz-tagging-directive = COPY
    when(headersForCopy.getHeaderString(TAG_DIRECTIVE_HEADER)).thenReturn("COPY");

    // With x-amz-tagging-directive = COPY with a different x-amz-tagging
    when(headersForCopy.getHeaderString(TAG_HEADER)).thenReturn("tag3=value3");
    copyResponse = objectEndpoint.put(DEST_BUCKET_NAME, destKey, CONTENT.length(), 1, body);
    assertEquals(200, copyResponse.getStatus());

    destKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(DEST_BUCKET_NAME).getKey(destKey);
    destKeyTags = destKeyDetails.getTags();

    // Since the x-amz-tagging-directive is COPY, we ignore the x-amz-tagging
    // header
    assertEquals(2, destKeyTags.size());
    assertEquals("value1", destKeyTags.get("tag1"));
    assertEquals("value2", destKeyTags.get("tag2"));

    // Copy object with x-amz-tagging-directive = REPLACE
    when(headersForCopy.getHeaderString(TAG_DIRECTIVE_HEADER)).thenReturn("REPLACE");
    copyResponse = objectEndpoint.put(DEST_BUCKET_NAME, destKey, CONTENT.length(), 1, body);
    assertEquals(200, copyResponse.getStatus());

    destKeyDetails = clientStub.getObjectStore()
        .getS3Bucket(DEST_BUCKET_NAME).getKey(destKey);
    destKeyTags = destKeyDetails.getTags();

    // Since the x-amz-tagging-directive is REPLACE, we replace the source key
    // tags with the one specified in the copy request
    assertEquals(1, destKeyTags.size());
    assertEquals("value3", destKeyTags.get("tag3"));
    assertThat(destKeyTags).doesNotContainKeys("tag1", "tag2");
  }

  @Test
  public void testCopyObjectWithInvalidTagCopyDirective() throws Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Copy object with invalid x-amz-tagging-directive
    HttpHeaders headersForCopy = Mockito.mock(HttpHeaders.class);
    when(headersForCopy.getHeaderString(TAG_DIRECTIVE_HEADER)).thenReturn("INVALID");
    try {
      objectEndpoint.put(DEST_BUCKET_NAME, "somekey", CONTENT.length(), 1, body);
    } catch (OS3Exception ex) {
      assertEquals(INVALID_ARGUMENT.getCode(), ex.getCode());
      assertThat(ex.getErrorMessage()).contains("The tagging copy directive specified is invalid");
      assertEquals(INVALID_ARGUMENT.getHttpCode(), ex.getHttpCode());
    }
  }

  @Test
  void testInvalidStorageType() {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("random");

    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
        BUCKET_NAME, KEY_NAME, CONTENT.length(), 1, body));
    assertEquals(S3ErrorTable.INVALID_STORAGE_CLASS.getErrorMessage(),
        e.getErrorMessage());
    assertEquals("random", e.getResource());
  }

  @Test
  void testEmptyStorageType() throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");

    objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT
            .length(), 1, body);
    OzoneKeyDetails key =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME)
            .getKey(KEY_NAME);

    //default type is set
    assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        key.getReplicationConfig());
  }

  @Test
  void testDirectoryCreation() throws IOException,
      OS3Exception {
    // GIVEN
    final String path = "dir/";

    // WHEN
    try (Response response = objectEndpoint.put(fsoBucket.getName(), path,
        0L, 0, null)) {
      assertEquals(HttpStatus.SC_OK, response.getStatus());
    }

    // THEN
    OzoneKeyDetails key = fsoBucket.getKey(path);
    assertThat(key.isFile()).as("directory").isFalse();
  }

  @Test
  void testDirectoryCreationOverFile() throws IOException, OS3Exception {
    // GIVEN
    final String path = "key";
    final ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.put(FSO_BUCKET_NAME, path, CONTENT.length(), 0, body);

    // WHEN
    final OS3Exception exception = assertThrows(OS3Exception.class,
        () -> objectEndpoint
            .put(FSO_BUCKET_NAME, path + "/", 0, 0, null)
            .close());

    // THEN
    assertEquals(S3ErrorTable.NO_OVERWRITE.getCode(), exception.getCode());
    assertEquals(S3ErrorTable.NO_OVERWRITE.getHttpCode(), exception.getHttpCode());
  }

  @Test
  public void testPutEmptyObject() throws IOException, OS3Exception {
    HttpHeaders headersWithTags = Mockito.mock(HttpHeaders.class);
    when(headersWithTags.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    String emptyString = "";
    ByteArrayInputStream body = new ByteArrayInputStream(emptyString.getBytes(UTF_8));
    objectEndpoint.setHeaders(headersWithTags);

    Response putResponse = objectEndpoint.put(BUCKET_NAME, KEY_NAME, emptyString.length(), 1, body);
    assertEquals(200, putResponse.getStatus());
    OzoneKeyDetails keyDetails = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    assertEquals(0, keyDetails.getDataSize());
  }
}
