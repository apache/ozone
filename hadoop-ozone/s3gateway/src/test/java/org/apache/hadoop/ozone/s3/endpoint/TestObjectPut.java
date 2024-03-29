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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import java.io.OutputStream;
import java.security.MessageDigest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = spy(new ObjectEndpoint());
    objectEndpoint.setClient(clientStub);
    objectEndpoint.setOzoneConfiguration(config);

    headers = mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);

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
    final String content = RandomStringUtils.randomAlphanumeric(length);
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes(UTF_8));
    bucket.setReplicationConfig(replication);

    //WHEN
    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME, length, 1, null, body);

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
  }

  @Test
  void testPutObjectContentLength() throws IOException, OS3Exception {
    // The contentLength specified when creating the Key should be the same as
    // the Content-Length, the key Commit will compare the Content-Length with
    // the actual length of the data written.
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    long dataSize = CONTENT.length();

    objectEndpoint.put(BUCKET_NAME, KEY_NAME, dataSize, 0, null, body);
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
    objectEndpoint.put(BUCKET_NAME, KEY_NAME, chunkedContent.length(), 0, null,
        new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));
    assertEquals(15, getKeyDataSize());
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
        chunkedContent.length(), 1, null,
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
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class)))
          .thenThrow(IOException.class);
      when(objectEndpoint.getMessageDigestInstance()).thenReturn(messageDigest);

      ByteArrayInputStream body =
          new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
      try {
        objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT
            .length(), 1, null, body);
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
        CONTENT.length(), 1, null, body);

    OzoneInputStream ozoneInputStream = clientStub.getObjectStore()
        .getS3Bucket(BUCKET_NAME)
        .readKey(KEY_NAME);

    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);
    OzoneKeyDetails keyDetails = clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);

    assertEquals(200, response.getStatus());
    assertEquals(CONTENT, keyContent);
    assertNotNull(keyDetails.getMetadata());
    assertThat(keyDetails.getMetadata().get(OzoneConsts.ETAG)).isNotEmpty();
    assertEquals("custom-value-1", keyDetails.getMetadata().get("custom-key-1"));
    assertEquals("custom-value-2", keyDetails.getMetadata().get("custom-key-2"));

    String sourceETag = keyDetails.getMetadata().get(OzoneConsts.ETAG);

    // This will be ignored since the copy directive is COPY
    metadataHeaders.putSingle(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-3", "custom-value-3");

    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    response = objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
        null, body);

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
    assertEquals("custom-value-1", destKeyDetails.getMetadata().get("custom-key-1"));
    assertEquals("custom-value-2", destKeyDetails.getMetadata().get("custom-key-2"));
    assertFalse(destKeyDetails.getMetadata().containsKey("custom-key-3"));

    // Now use REPLACE metadata directive (default) and remove some custom metadata used in the source key
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("REPLACE");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-1");
    metadataHeaders.remove(CUSTOM_METADATA_HEADER_PREFIX + "custom-key-2");

    response = objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
          null, body);

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
    assertFalse(keyDetails.getMetadata().get(OzoneConsts.ETAG).isEmpty());
    // Source key eTag should remain unchanged and the dest key should have
    // the same Etag since the key content is the same
    assertEquals(sourceETag, sourceKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertEquals(sourceETag, destKeyDetails.getMetadata().get(OzoneConsts.ETAG));
    assertFalse(destKeyDetails.getMetadata().containsKey("custom-key-1"));
    assertFalse(destKeyDetails.getMetadata().containsKey("custom-key-2"));
    assertEquals("custom-value-3", destKeyDetails.getMetadata().get("custom-key-3"));


    // wrong copy metadata directive
    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("INVALID");
    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
                  DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1, null, body),
          "test copy object failed");
    assertThat(e.getHttpCode()).isEqualTo(400);
    assertThat(e.getCode()).isEqualTo("InvalidArgument");
    assertThat(e.getErrorMessage()).contains("The metadata directive specified is invalid");

    when(headers.getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER)).thenReturn("COPY");

    // source bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          NO_SUCH_BUCKET + "/"  + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(DEST_BUCKET_NAME,
          DEST_KEY, CONTENT.length(), 1, null, body), "test copy object failed");
    assertThat(e.getCode()).contains("NoSuchBucket");

    // dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(NO_SUCH_BUCKET,
        DEST_KEY, CONTENT.length(), 1, null, body), "test copy object failed");
    assertTrue(e.getCode().contains("NoSuchBucket"));

    //Both source and dest bucket not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        NO_SUCH_BUCKET + "/" + urlEncode(KEY_NAME));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(NO_SUCH_BUCKET,
        DEST_KEY, CONTENT.length(), 1, null, body), "test copy object failed");
    assertTrue(e.getCode().contains("NoSuchBucket"));

    // source key not found
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        BUCKET_NAME + "/" + urlEncode(NO_SUCH_BUCKET));
    e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
        "nonexistent", KEY_NAME, CONTENT.length(), 1, null, body),
        "test copy object failed");
    assertTrue(e.getCode().contains("NoSuchBucket"));
  }

  @Test
  public void testCopyObjectMessageDigestResetDuringException() throws IOException, OS3Exception {
    // Put object in to source bucket
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));

    Response response = objectEndpoint.put(BUCKET_NAME, KEY_NAME,
        CONTENT.length(), 1, null, body);

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
      mocked.when(() -> IOUtils.copyLarge(any(InputStream.class), any(OutputStream.class)))
          .thenThrow(IOException.class);

      // Add copy header, and then call put
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          BUCKET_NAME  + "/" + urlEncode(KEY_NAME));

      try {
        objectEndpoint.put(DEST_BUCKET_NAME, DEST_KEY, CONTENT.length(), 1,
            null, body);
        fail("Should throw IOException");
      } catch (IOException ignored) {
        // Verify that the message digest is reset so that the instance can be reused for the
        // next request in the same thread
        verify(messageDigest, times(1)).reset();
      }
    }
  }

  @Test
  void testInvalidStorageType() {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("random");

    OS3Exception e = assertThrows(OS3Exception.class, () -> objectEndpoint.put(
        BUCKET_NAME, KEY_NAME, CONTENT.length(), 1, null, body));
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getErrorMessage(),
        e.getErrorMessage());
    assertEquals("random", e.getResource());
  }

  @Test
  void testEmptyStorageType() throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");

    objectEndpoint.put(BUCKET_NAME, KEY_NAME, CONTENT
            .length(), 1, null, body);
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
        0L, 0, "", null)) {
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
    objectEndpoint.put(FSO_BUCKET_NAME, path, CONTENT.length(), 0, "", body);

    // WHEN
    final OS3Exception exception = assertThrows(OS3Exception.class,
        () -> objectEndpoint
            .put(FSO_BUCKET_NAME, path + "/", 0, 0, "", null)
            .close());

    // THEN
    assertEquals(S3ErrorTable.NO_OVERWRITE.getCode(), exception.getCode());
    assertEquals(S3ErrorTable.NO_OVERWRITE.getHttpCode(), exception.getHttpCode());
  }
}
