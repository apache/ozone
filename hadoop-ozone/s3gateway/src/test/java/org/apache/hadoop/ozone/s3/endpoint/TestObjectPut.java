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

import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.putDir;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_TAG;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STREAMING_AWS4_HMAC_SHA256_PAYLOAD;
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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

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
  private static final String NONEXISTENT_BUCKET = "nonexist";

  private ObjectEndpoint objectEndpoint;
  private HttpHeaders headers;
  private OzoneBucket bucket;
  private OzoneBucket destBucket;
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
    headers = newMockHttpHeaders();
    objectEndpoint = spy(EndpointBuilder.newObjectEndpointBuilder().setHeaders(headers).build());

    // Create buckets
    OzoneClient clientStub = objectEndpoint.getClient();
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME);
    clientStub.getObjectStore().createS3Bucket(DEST_BUCKET_NAME);
    destBucket = clientStub.getObjectStore().getS3Bucket(DEST_BUCKET_NAME);

    String volumeName = objectEndpoint.getOzoneConfiguration().get(OzoneConfigKeys.OZONE_S3_VOLUME_NAME,
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
  void testPutObject(int length, ReplicationConfig replication) throws Exception {
    //GIVEN
    final String content = RandomStringUtils.secure().nextAlphanumeric(length);
    bucket.setReplicationConfig(replication);

    //WHEN
    assertSucceeds(() -> putObject(content));

    //THEN
    OzoneKeyDetails keyDetails = assertKeyContent(bucket, KEY_NAME, content);
    assertEquals(content.length(), keyDetails.getDataSize());
    assertEquals(replication, keyDetails.getReplicationConfig());
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    assertThat(keyDetails.getTags()).isEmpty();
  }

  @Test
  public void testPutObjectWithTags() throws Exception {
    when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");

    assertSucceeds(() -> putObject(CONTENT));

    Map<String, String> tags = bucket.getKey(KEY_NAME).getTags();
    assertEquals(2, tags.size());
    assertEquals("value1", tags.get("tag1"));
    assertEquals("value2", tags.get("tag2"));
  }

  @Test
  public void testPutObjectWithOnlyTagKey() {
    // Try to send with only the key (no value)
    when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1");

    OS3Exception ex = assertErrorResponse(INVALID_TAG, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains("Some tag values are not specified");
  }

  @Test
  public void testPutObjectWithDuplicateTagKey() {
    when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag1=value2");

    OS3Exception ex = assertErrorResponse(INVALID_TAG, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains("There are tags with duplicate tag keys");
  }

  @Test
  public void testPutObjectWithLongTagKey() {
    String longTagKey = StringUtils.repeat('k', TAG_KEY_LENGTH_LIMIT + 1);
    when(headers.getHeaderString(TAG_HEADER)).thenReturn(longTagKey + "=value1");

    OS3Exception ex = assertErrorResponse(INVALID_TAG, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains("The tag key exceeds the maximum length");
  }

  @Test
  public void testPutObjectWithLongTagValue() {
    String longTagValue = StringUtils.repeat('v', TAG_VALUE_LENGTH_LIMIT + 1);
    when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=" + longTagValue);

    OS3Exception ex = assertErrorResponse(INVALID_TAG, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains("The tag value exceeds the maximum length");
  }

  @Test
  public void testPutObjectWithTooManyTags() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TAG_NUM_LIMIT + 1; i++) {
      sb.append(String.format("tag%d=value%d", i, i));
      if (i < TAG_NUM_LIMIT) {
        sb.append('&');
      }
    }
    when(headers.getHeaderString(TAG_HEADER)).thenReturn(sb.toString());

    OS3Exception ex = assertErrorResponse(INVALID_TAG, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains("exceeded the maximum number of tags");
  }

  @Test
  void testPutObjectWithSignedChunks() throws Exception {
    //GIVEN
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn(STREAMING_AWS4_HMAC_SHA256_PAYLOAD);
    when(headers.getHeaderString(DECODED_CONTENT_LENGTH_HEADER))
        .thenReturn("15");

    //WHEN
    assertSucceeds(() -> putObject(chunkedContent));

    //THEN
    OzoneKeyDetails keyDetails = assertKeyContent(bucket, KEY_NAME, "1234567890abcde");
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    assertEquals(15, keyDetails.getDataSize());
  }

  @Test
  public void testPutObjectMessageDigestResetDuringException() {
    MessageDigest messageDigest = mock(MessageDigest.class);
    try (MockedStatic<IOUtils> mocked = mockStatic(IOUtils.class)) {
      // For example, EOFException during put-object due to client cancelling the operation before it completes
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class), anyLong(),
              anyLong(), any(byte[].class)))
          .thenThrow(IOException.class);
      when(objectEndpoint.getMessageDigestInstance()).thenReturn(messageDigest);

      assertThrows(IOException.class, () -> putObject(CONTENT).close());

      // Verify that the message digest is reset so that the instance can be reused for the
      // next request in the same thread
      verify(messageDigest, times(1)).reset();
    }
  }

  @Test
  void testCopyObject() throws Exception {
    // Put object in to source bucket

    // Add some custom metadata
    Map<String, String> customMetadata = ImmutableMap.of(
        "custom-key-1", "custom-value-1",
        "custom-key-2", "custom-value-2");
    MultivaluedMap<String, String> metadataHeaders = new MultivaluedHashMap<>();
    customMetadata.forEach((k, v) -> metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + k, v));
    when(headers.getRequestHeaders()).thenReturn(metadataHeaders);
    // Add COPY metadata directive (default)
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("COPY");

    assertSucceeds(() -> putObject(CONTENT));

    OzoneKeyDetails keyDetails = assertKeyContent(bucket, KEY_NAME, CONTENT);
    assertNotNull(keyDetails.getMetadata());
    String sourceETag = keyDetails.getMetadata().get(OzoneConsts.ETAG);
    assertThat(sourceETag).isNotEmpty();
    assertThat(keyDetails.getMetadata()).containsAllEntriesOf(customMetadata);

    // This will be ignored since the copy directive is COPY
    metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-3", "custom-value-3");

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    assertSucceeds(() -> put(objectEndpoint, DEST_BUCKET_NAME, DEST_KEY, CONTENT));

    // Check destination key and response
    OzoneKeyDetails destKeyDetails = assertKeyContent(destBucket, DEST_KEY, CONTENT);
    keyDetails = bucket.getKey(KEY_NAME);
    // Source key eTag should remain unchanged and the dest key should have
    // the same Etag since the key content is the same
    assertEquals(sourceETag, keyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertEquals(sourceETag, destKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertThat(destKeyDetails.getMetadata())
        .containsAllEntriesOf(customMetadata)
        .doesNotContainKey("custom-key-3");

    // Now use REPLACE metadata directive (default) and remove some custom metadata used in the source key
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("REPLACE");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-1");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-2");

    assertSucceeds(() -> put(objectEndpoint, DEST_BUCKET_NAME, DEST_KEY, CONTENT));

    destKeyDetails = assertKeyContent(destBucket, DEST_KEY, CONTENT);
    assertNotNull(keyDetails.getMetadata());
    // Source key eTag should remain unchanged and the dest key should have
    // the same Etag since the key content is the same
    assertEquals(sourceETag, keyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertEquals(sourceETag, destKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertThat(destKeyDetails.getMetadata())
        .doesNotContainKeys("custom-key-1", "custom-key-2")
        .containsEntry("custom-key-3", "custom-value-3");


    // wrong copy metadata directive
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("INVALID");
    OS3Exception e = assertErrorResponse(INVALID_ARGUMENT,
        () -> put(objectEndpoint, DEST_BUCKET_NAME, DEST_KEY, CONTENT));
    assertThat(e.getErrorMessage()).contains("The metadata copy directive specified is invalid");

    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("COPY");

    // source and dest same
    e = assertErrorResponse(INVALID_REQUEST, () -> putObject(CONTENT));
    assertThat(e.getErrorMessage()).contains("This copy request is illegal");

    // source bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        NONEXISTENT_BUCKET + "/"  + urlEncode(KEY_NAME));
    assertErrorResponse(NO_SUCH_BUCKET,
        () -> put(objectEndpoint, DEST_BUCKET_NAME, DEST_KEY, CONTENT));

    // dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));
    assertErrorResponse(NO_SUCH_BUCKET,
        () -> put(objectEndpoint, NONEXISTENT_BUCKET, DEST_KEY, CONTENT));

    //Both source and dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        NONEXISTENT_BUCKET + "/" + urlEncode(KEY_NAME));
    assertErrorResponse(NO_SUCH_BUCKET,
        () -> put(objectEndpoint, NONEXISTENT_BUCKET, DEST_KEY, CONTENT));

    // source key not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(NONEXISTENT_BUCKET));
    assertErrorResponse(NO_SUCH_BUCKET,
        () -> put(objectEndpoint, "nonexistent", KEY_NAME, CONTENT));
  }

  @Test
  public void testCopyObjectMessageDigestResetDuringException() throws Exception {
    assertSucceeds(() -> putObject(CONTENT));

    OzoneKeyDetails keyDetails = assertKeyContent(bucket, KEY_NAME, CONTENT);
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

      assertThrows(IOException.class, () -> putObject(DEST_BUCKET_NAME, DEST_KEY).close());
      // Verify that the message digest is reset so that the instance can be reused for the
      // next request in the same thread
      verify(messageDigest, times(1)).reset();
    }
  }

  @Test
  public void testCopyObjectWithTags() throws Exception {
    // Put object in to source bucket
    HttpHeaders headersForPut = newMockHttpHeaders();
    when(headersForPut.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    objectEndpoint.setHeaders(headersForPut);

    String sourceKeyName = "sourceKey";

    assertSucceeds(() -> putObject(BUCKET_NAME, sourceKeyName));

    Map<String, String> tags = bucket.getKey(sourceKeyName).getTags();
    assertEquals(2, tags.size());
    assertEquals("value1", tags.get("tag1"));
    assertEquals("value2", tags.get("tag2"));

    // Copy object without x-amz-tagging-directive (default to COPY)
    String destKey = "key=value/2";
    HttpHeaders headersForCopy = newMockHttpHeaders();
    when(headersForCopy.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME  + "/" + urlEncode(sourceKeyName));
    objectEndpoint.setHeaders(headersForCopy);

    assertSucceeds(() -> putObject(DEST_BUCKET_NAME, destKey));

    OzoneKeyDetails destKeyDetails = destBucket.getKey(destKey);

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
    assertSucceeds(() -> putObject(DEST_BUCKET_NAME, destKey));

    destKeyDetails = destBucket.getKey(destKey);
    destKeyTags = destKeyDetails.getTags();

    // Since the x-amz-tagging-directive is COPY, we ignore the x-amz-tagging
    // header
    assertEquals(2, destKeyTags.size());
    assertEquals("value1", destKeyTags.get("tag1"));
    assertEquals("value2", destKeyTags.get("tag2"));

    // Copy object with x-amz-tagging-directive = REPLACE
    when(headersForCopy.getHeaderString(TAG_DIRECTIVE_HEADER)).thenReturn("REPLACE");
    assertSucceeds(() -> putObject(DEST_BUCKET_NAME, destKey));

    destKeyDetails = destBucket.getKey(destKey);
    destKeyTags = destKeyDetails.getTags();

    // Since the x-amz-tagging-directive is REPLACE, we replace the source key
    // tags with the one specified in the copy request
    assertEquals(1, destKeyTags.size());
    assertEquals("value3", destKeyTags.get("tag3"));
    assertThat(destKeyTags).doesNotContainKeys("tag1", "tag2");

    // Copy object with invalid x-amz-tagging-directive
    when(headersForCopy.getHeaderString(TAG_DIRECTIVE_HEADER)).thenReturn("INVALID");
    OS3Exception e = assertErrorResponse(INVALID_ARGUMENT,
        () -> put(objectEndpoint, DEST_BUCKET_NAME, "somekey", CONTENT));
    assertThat(e.getErrorMessage()).contains("The tagging copy directive specified is invalid");
  }

  @Test
  void testInvalidStorageType() {
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("random");

    OS3Exception e = assertErrorResponse(S3ErrorTable.INVALID_STORAGE_CLASS, () -> putObject(CONTENT));
    assertEquals("random", e.getResource());
  }

  @Test
  void testEmptyStorageType() throws Exception {
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");

    assertSucceeds(() -> putObject(CONTENT));

    //default type is set
    assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        bucket.getKey(KEY_NAME).getReplicationConfig());
  }

  @Test
  void testDirectoryCreation() throws Exception {
    // GIVEN
    final String path = "dir/";

    // WHEN
    assertSucceeds(() -> putDir(objectEndpoint, fsoBucket.getName(), path));

    // THEN
    OzoneKeyDetails key = fsoBucket.getKey(path);
    assertThat(key.isFile()).as("directory").isFalse();
  }

  @Test
  void testDirectoryCreationOverFile() throws Exception {
    // GIVEN
    final String path = "key";
    assertSucceeds(() -> putObject(FSO_BUCKET_NAME, path));

    assertErrorResponse(S3ErrorTable.NO_OVERWRITE,
        () -> putDir(objectEndpoint, FSO_BUCKET_NAME, path + "/"));
  }

  @Test
  public void testPutEmptyObject() throws Exception {
    assertSucceeds(() -> putObject(""));
    assertEquals(0, bucket.getKey(KEY_NAME).getDataSize());
  }

  @Test
  public void testPutObjectWithContentMD5() throws Exception {
    // GIVEN
    byte[] contentBytes = CONTENT.getBytes(StandardCharsets.UTF_8);
    byte[] md5Bytes = MessageDigest.getInstance("MD5").digest(contentBytes);
    String md5Base64 = Base64.getEncoder().encodeToString(md5Bytes);

    when(headers.getHeaderString("Content-MD5")).thenReturn(md5Base64);

    // WHEN
    assertSucceeds(() -> putObject(CONTENT));

    // THEN
    OzoneKeyDetails keyDetails = assertKeyContent(bucket, KEY_NAME, CONTENT);
    assertEquals(CONTENT.length(), keyDetails.getDataSize());
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
  }


  public static Stream<Arguments> wrongContentMD5Provider() throws NoSuchAlgorithmException {
    byte[] wrongContentBytes = "wrong".getBytes(StandardCharsets.UTF_8);
    byte[] wrongMd5Bytes = MessageDigest.getInstance("MD5").digest(wrongContentBytes);
    String wrongMd5Base64 = Base64.getEncoder().encodeToString(wrongMd5Bytes);
    return Stream.of(
        Arguments.arguments(wrongMd5Base64),
        Arguments.arguments("invalid-base64")
    );
  }

  @ParameterizedTest
  @MethodSource("wrongContentMD5Provider")
  public void testPutObjectWithWrongContentMD5(String wrongContentMD5) throws Exception {

    // WHEN
    when(headers.getHeaderString("Content-MD5")).thenReturn(wrongContentMD5);

    // WHEN/THEN
    OS3Exception ex = assertErrorResponse(S3ErrorTable.BAD_DIGEST, () -> putObject(CONTENT));
    assertThat(ex.getErrorMessage()).contains(S3ErrorTable.BAD_DIGEST.getErrorMessage());
  }

  private HttpHeaders newMockHttpHeaders() {
    HttpHeaders httpHeaders = mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");
    return httpHeaders;
  }

  /** Put object at {@code bucketName}/{@code keyName} with pre-defined {@link #CONTENT}. */
  private Response putObject(String bucketName, String keyName) throws IOException, OS3Exception {
    return put(objectEndpoint, bucketName, keyName, CONTENT);
  }

  /** Put object at {@link #BUCKET_NAME}/{@link #KEY_NAME} with the specified content. */
  private Response putObject(String content) throws IOException, OS3Exception {
    return put(objectEndpoint, BUCKET_NAME, KEY_NAME, content);
  }
}
