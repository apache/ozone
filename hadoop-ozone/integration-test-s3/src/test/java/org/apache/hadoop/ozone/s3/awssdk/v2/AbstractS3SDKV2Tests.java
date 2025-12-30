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

package org.apache.hadoop.ozone.s3.awssdk.v2;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.calculateDigest;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.createFile;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_KEY_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_NUM_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_VALUE_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static software.amazon.awssdk.core.sync.RequestBody.fromString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils;
import org.apache.hadoop.ozone.s3.endpoint.S3Owner;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.NonHATests;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.CompleteMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.CreateMultipartUploadPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.DeleteObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.HeadBucketPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.HeadObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedCreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedDeleteObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedHeadBucketRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedHeadObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedUploadPartRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.ResumableFileDownload;
import software.amazon.awssdk.utils.IoUtils;

/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from
 * - https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/javav2/example_code/s3/src/main/java/com/example/s3
 * - https://github.com/ceph/s3-tests
 *
 * TODO: Add tests to different types of S3 client (Async client, CRT-based client)
 *  See:
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3.html
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/asynchronous.html
 *   - https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractS3SDKV2Tests extends OzoneTestBase implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;
  private S3Client s3Client;
  private S3AsyncClient s3AsyncClient;

  @BeforeAll
  void createClient() throws Exception {
    cluster = cluster();
    S3ClientFactory s3Factory = new S3ClientFactory(cluster.getConf());
    s3Client = s3Factory.createS3ClientV2();
    s3AsyncClient = s3Factory.createS3AsyncClientV2();
  }

  @AfterAll
  void closeClient() {
    if (s3Client != null) {
      s3Client.close();
    }
    if (s3AsyncClient != null) {
      s3AsyncClient.close();
    }
  }

  @Test
  public void listBuckets() throws Exception {
    final String bucketName = getBucketName();
    final String expectedOwner = UserGroupInformation.getCurrentUser().getUserName();

    s3Client.createBucket(b -> b.bucket(bucketName));

    ListBucketsResponse syncResponse = s3Client.listBuckets();
    assertEquals(1, syncResponse.buckets().size());
    assertEquals(bucketName, syncResponse.buckets().get(0).name());
    assertEquals(expectedOwner, syncResponse.owner().displayName());
    assertEquals(S3Owner.DEFAULT_S3OWNER_ID, syncResponse.owner().id());
  }

  @Test
  public void testPutObject() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(bucketName));

    PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
            .bucket(bucketName)
            .key(keyName),
        RequestBody.fromString(content));

    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", putObjectResponse.eTag());

    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(keyName)
    );
    GetObjectResponse getObjectResponse = objectBytes.response();

    assertEquals(content, objectBytes.asUtf8String());
    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", getObjectResponse.eTag());
  }

  @Test
  public void testListObjectsMany() throws Exception {
    testListObjectsMany(false);
  }

  @Test
  public void testListObjectsManyV2() throws Exception {
    testListObjectsMany(true);
  }

  private void testListObjectsMany(boolean isListV2) throws Exception {
    final String bucketName = getBucketName();
    s3Client.createBucket(b -> b.bucket(bucketName));
    final List<String> keyNames = Arrays.asList(
        getKeyName("1"),
        getKeyName("2"),
        getKeyName("3")
    );
    final List<String> keyNamesWithoutETag = Arrays.asList(
        getKeyName("4"),
        getKeyName("5")
    );
    final Map<String, String> keyToEtag = new HashMap<>();
    for (String keyName: keyNames) {
      PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
          .bucket(bucketName)
          .key(keyName),
          RequestBody.fromString(RandomStringUtils.secure().nextAlphanumeric(5)));
      keyToEtag.put(keyName, putObjectResponse.eTag());
    }
    try (OzoneClient ozoneClient = cluster.newClient()) {
      ObjectStore store = ozoneClient.getObjectStore();

      OzoneVolume volume = store.getS3Volume();
      OzoneBucket bucket = volume.getBucket(bucketName);

      for (String keyNameWithoutETag : keyNamesWithoutETag) {
        byte[] valueBytes = RandomStringUtils.secure().nextAlphanumeric(5).getBytes(StandardCharsets.UTF_8);
        try (OzoneOutputStream out = bucket.createKey(keyNameWithoutETag,
            valueBytes.length,
            ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
            Collections.emptyMap())) {
          out.write(valueBytes);
        }
      }
    }

    List<S3Object> s3Objects;
    String continuationToken;
    if (isListV2) {
      ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
          .bucket(bucketName)
          .maxKeys(2)
          .build();
      ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
      s3Objects = listObjectsResponse.contents();
      assertEquals(bucketName, listObjectsResponse.name());
      assertTrue(listObjectsResponse.isTruncated());
      continuationToken = listObjectsResponse.nextContinuationToken();
    } else {
      ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
          .bucket(bucketName)
          .maxKeys(2)
          .build();
      ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
      s3Objects = listObjectsResponse.contents();
      assertEquals(bucketName, listObjectsResponse.name());
      assertTrue(listObjectsResponse.isTruncated());
      continuationToken = listObjectsResponse.nextMarker();
    }
    assertThat(s3Objects).hasSize(2);
    assertEquals(s3Objects.stream()
        .map(S3Object::key).collect(Collectors.toList()),
        keyNames.subList(0, 2));
    for (S3Object s3Object : s3Objects) {
      assertEquals(keyToEtag.get(s3Object.key()), s3Object.eTag());
    }

    // Include both keys with and without ETag
    if (isListV2) {
      ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
          .bucket(bucketName)
          .maxKeys(5)
          .continuationToken(continuationToken)
          .build();
      ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
      s3Objects = listObjectsResponse.contents();
      assertEquals(bucketName, listObjectsResponse.name());
      assertFalse(listObjectsResponse.isTruncated());
    } else {
      ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
          .bucket(bucketName)
          .maxKeys(5)
          .marker(continuationToken)
          .build();
      ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
      s3Objects = listObjectsResponse.contents();
      assertEquals(bucketName, listObjectsResponse.name());
      assertFalse(listObjectsResponse.isTruncated());
    }

    assertThat(s3Objects).hasSize(3);
    assertEquals(keyNames.get(2), s3Objects.get(0).key());
    assertEquals(keyNamesWithoutETag.get(0), s3Objects.get(1).key());
    assertEquals(keyNamesWithoutETag.get(1), s3Objects.get(2).key());
    for (S3Object s3Object : s3Objects) {
      assertEquals(keyToEtag.get(s3Object.key()), s3Object.eTag());
    }
  }

  @Test
  public void testCopyObject() {
    final String sourceBucketName = getBucketName("source");
    final String destBucketName = getBucketName("dest");
    final String sourceKey = getKeyName("source");
    final String destKey = getKeyName("dest");
    final String content = "bar";
    s3Client.createBucket(b -> b.bucket(sourceBucketName));
    s3Client.createBucket(b -> b.bucket(destBucketName));

    PutObjectResponse putObjectResponse = s3Client.putObject(b -> b
            .bucket(sourceBucketName)
            .key(sourceKey),
        RequestBody.fromString(content));

    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", putObjectResponse.eTag());

    CopyObjectRequest copyReq = CopyObjectRequest.builder()
        .sourceBucket(sourceBucketName)
        .sourceKey(sourceKey)
        .destinationBucket(destBucketName)
        .destinationKey(destKey)
        .build();

    CopyObjectResponse copyObjectResponse = s3Client.copyObject(copyReq);
    assertEquals("\"37b51d194a7513e45b56f6524f2d51f2\"", copyObjectResponse.copyObjectResult().eTag());
  }

  @Test
  public void testPutAndGetObjectTagging() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "test content";
    s3Client.createBucket(b -> b.bucket(bucketName));

    s3Client.putObject(b -> b
            .bucket(bucketName)
            .key(keyName),
        RequestBody.fromString(content));

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("env").value("test").build(),
        Tag.builder().key("project").value("ozone").build()
    );

    s3Client.putObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName)
        .tagging(Tagging.builder().tagSet(tags).build()));

    GetObjectTaggingResponse getResponse = s3Client.getObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName));

    assertEquals(tags.size(), getResponse.tagSet().size());
    Map<String, String> tagMap = getResponse.tagSet().stream()
        .collect(Collectors.toMap(Tag::key, Tag::value));
    assertEquals("test", tagMap.get("env"));
    assertEquals("ozone", tagMap.get("project"));
  }

  @Test
  public void testDeleteObjectTagging() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "test content";
    s3Client.createBucket(b -> b.bucket(bucketName));

    s3Client.putObject(b -> b
            .bucket(bucketName)
            .key(keyName),
        RequestBody.fromString(content));

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("temp").value("data").build()
    );

    s3Client.putObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName)
        .tagging(Tagging.builder().tagSet(tags).build()));

    GetObjectTaggingResponse beforeDelete = s3Client.getObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName));
    assertEquals(1, beforeDelete.tagSet().size());

    s3Client.deleteObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName));

    GetObjectTaggingResponse afterDelete = s3Client.getObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName));
    assertTrue(afterDelete.tagSet().isEmpty());
  }

  @Test
  public void testPutObjectTaggingExceedsLimit() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));
    s3Client.putObject(b -> b.bucket(bucketName).key(keyName),
        RequestBody.fromString("content"));

    List<Tag> tags = new ArrayList<>();
    for (int i = 1; i <= TAG_NUM_LIMIT + 1; i++) {
      tags.add(Tag.builder().key("key" + i).value("value" + i).build());
    }

    S3Exception exception = assertThrows(S3Exception.class, () ->
        s3Client.putObjectTagging(b -> b
            .bucket(bucketName)
            .key(keyName)
            .tagging(Tagging.builder().tagSet(tags).build())));
    assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, exception.statusCode());
  }

  @Test
  public void testPutObjectTaggingReplacesExistingTags() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));
    s3Client.putObject(b -> b.bucket(bucketName).key(keyName),
        RequestBody.fromString("content"));

    List<Tag> initialTags = Arrays.asList(
        Tag.builder().key("tag1").value("value1").build(),
        Tag.builder().key("tag2").value("value2").build()
    );
    s3Client.putObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName)
        .tagging(Tagging.builder().tagSet(initialTags).build()));

    List<Tag> replacementTags = Arrays.asList(
        Tag.builder().key("tag3").value("value3").build()
    );
    s3Client.putObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName)
        .tagging(Tagging.builder().tagSet(replacementTags).build()));

    GetObjectTaggingResponse response = s3Client.getObjectTagging(b -> b
        .bucket(bucketName)
        .key(keyName));
    assertEquals(1, response.tagSet().size());
    Map<String, String> tagMap = response.tagSet().stream()
        .collect(Collectors.toMap(Tag::key, Tag::value));
    assertEquals("value3", tagMap.get("tag3"));
    assertFalse(tagMap.containsKey("tag1"));
    assertFalse(tagMap.containsKey("tag2"));
  }

  private static String repeatChar(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }

  private static Stream<Arguments> invalidTagConstraintsProvider() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(Tag.builder().key(repeatChar('a', TAG_KEY_LENGTH_LIMIT + 1)).value("value").build()),
            HTTP_BAD_REQUEST
        ),
        Arguments.of(
            Arrays.asList(Tag.builder().key("valid-key").value(repeatChar('b', TAG_VALUE_LENGTH_LIMIT + 1)).build()),
            HTTP_BAD_REQUEST
        ),
        Arguments.of(
            Arrays.asList(Tag.builder().key("t$ag@#invalid").value("value").build()),
            HTTP_BAD_REQUEST
        ),
        Arguments.of(
            Arrays.asList(Tag.builder().key("aws:test").value("value").build()),
            HTTP_BAD_REQUEST
        )
    );
  }

  @ParameterizedTest
  @MethodSource("invalidTagConstraintsProvider")
  public void testPutObjectTaggingInvalidConstraints(List<Tag> invalidTags, int expectedStatusCode) {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));
    s3Client.putObject(b -> b.bucket(bucketName).key(keyName),
        RequestBody.fromString("content"));

    S3Exception exception = assertThrows(S3Exception.class, () ->
        s3Client.putObjectTagging(b -> b
            .bucket(bucketName)
            .key(keyName)
            .tagging(Tagging.builder().tagSet(invalidTags).build())));
    assertEquals(expectedStatusCode, exception.statusCode());
  }

  @Test
  public void testPutAndGetObjectTaggingOnNonExistentObject() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("env").value("test").build()
    );

    NoSuchKeyException exception = assertThrows(NoSuchKeyException.class, () ->
        s3Client.putObjectTagging(b -> b
            .bucket(bucketName)
            .key(keyName)
            .tagging(Tagging.builder().tagSet(tags).build())));
    assertEquals(HttpURLConnection.HTTP_NOT_FOUND, exception.statusCode());

    exception = assertThrows(NoSuchKeyException.class, () ->
        s3Client.getObjectTagging(b -> b
            .bucket(bucketName)
            .key(keyName)));
    assertEquals(HttpURLConnection.HTTP_NOT_FOUND, exception.statusCode());
  }

  @Test
  public void testDeleteObjectTaggingOnNonExistentObject() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(b -> b.bucket(bucketName));

    NoSuchKeyException exception = assertThrows(NoSuchKeyException.class, () ->
        s3Client.deleteObjectTagging(b -> b
            .bucket(bucketName)
            .key(keyName)));
    assertEquals(HttpURLConnection.HTTP_NOT_FOUND, exception.statusCode());
  }

  @Test
  public void testLowLevelMultipartUpload(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("key1", "value1");
    userMetadata.put("key2", "value2");

    List<Tag> tags = Arrays.asList(
        Tag.builder().key("tag1").value("value1").build(),
        Tag.builder().key("tag2").value("value2").build()
    );

    s3Client.createBucket(b -> b.bucket(bucketName));

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (25 * MB));

    multipartUpload(bucketName, keyName, multipartUploadFile, (int) (5 * MB), userMetadata, tags);

    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(keyName)
    );

    GetObjectResponse getObjectResponse = objectBytes.response();

    assertEquals(tags.size(), getObjectResponse.tagCount());

    HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(bucketName).key(keyName));
    assertTrue(headObjectResponse.hasMetadata());
    assertEquals(userMetadata, headObjectResponse.metadata());
  }

  @Test
  public void testResumableDownloadWithEtagMismatch() throws Exception {
    // Arrange
    final String bucketName = getBucketName("resumable");
    final String keyName = getKeyName("resumable");
    final String fileContent = "This is a test file for resumable download.";
    s3Client.createBucket(b -> b.bucket(bucketName));
    s3Client.putObject(b -> b.bucket(bucketName).key(keyName), RequestBody.fromString(fileContent));

    // Prepare a temp file for download
    Path downloadPath = Files.createTempFile("downloaded", ".txt");

    // Set up S3TransferManager
    try (S3TransferManager transferManager =
             S3TransferManager.builder().s3Client(s3AsyncClient).build()) {

      // First download
      DownloadFileRequest downloadRequest = DownloadFileRequest.builder()
          .getObjectRequest(b -> b.bucket(bucketName).key(keyName))
          .destination(downloadPath)
          .build();
      FileDownload download = transferManager.downloadFile(downloadRequest);
      ResumableFileDownload resumableFileDownload = download.pause();

      // Simulate etag mismatch by modifying the file in S3
      final String newContent = "This is new content to cause etag mismatch.";
      s3Client.putObject(b -> b.bucket(bucketName).key(keyName), RequestBody.fromString(newContent));

      // Resume download
      FileDownload resumedDownload = transferManager.resumeDownloadFile(resumableFileDownload);
      resumedDownload.completionFuture().get();

      String downloadedContent = new String(Files.readAllBytes(downloadPath), StandardCharsets.UTF_8);
      assertEquals(newContent, downloadedContent);

      File downloadFile = downloadPath.toFile();
      assertTrue(downloadFile.delete());
    }
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class PresignedUrlTests {
    private static final String CONTENT = "bar";
    private static final String BUCKET_NAME = "v2-presigned-url-bucket";
    private final SdkHttpClient sdkHttpClient = ApacheHttpClient.create();
    // The URL will expire in 10 minutes.
    private final Duration duration = Duration.ofMinutes(10);
    private S3Presigner presigner;

    @BeforeAll
    public void setup() {
      s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
      presigner = S3Presigner.builder()
          // TODO: Find a way to retrieve the path style configuration from S3Client instead
          .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
          .endpointOverride(s3Client.serviceClientConfiguration().endpointOverride().get())
          .region(s3Client.serviceClientConfiguration().region())
          .credentialsProvider(s3Client.serviceClientConfiguration().credentialsProvider()).build();
    }

    @AfterAll
    public void tearDown() {
      presigner.close();
      sdkHttpClient.close();
    }

    @Test
    public void testPresignedUrlGet() throws Exception {
      final String keyName = getKeyName();

      s3Client.putObject(b -> b
              .bucket(BUCKET_NAME)
              .key(keyName),
          RequestBody.fromString(CONTENT));

      GetObjectRequest objectRequest = GetObjectRequest.builder().bucket(BUCKET_NAME).key(keyName).build();

      GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
          .signatureDuration(duration)
          .getObjectRequest(objectRequest)
          .build();

      PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);

      // Download the object using HttpUrlConnection (since v1.1)
      // Capture the response body to a byte array.
      URL presignedUrl = presignedRequest.url();
      HttpURLConnection connection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "GET", null, null);
      // Download the result of executing the request.
      try (InputStream s3is = connection.getInputStream();
           ByteArrayOutputStream bos = new ByteArrayOutputStream(CONTENT.getBytes(StandardCharsets.UTF_8).length)) {
        IoUtils.copy(s3is, bos);
        assertEquals(CONTENT, bos.toString("UTF-8"));
      } finally {
        connection.disconnect();
      }

      // Use the AWS SDK for Java SdkHttpClient class to do the download
      SdkHttpRequest request = SdkHttpRequest.builder().method(SdkHttpMethod.GET).uri(presignedUrl.toURI()).build();

      HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(request).build();

      try (ByteArrayOutputStream bos = new ByteArrayOutputStream(CONTENT.getBytes(StandardCharsets.UTF_8).length)) {
        HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
        assertTrue(response.responseBody().isPresent(),
            () -> "The presigned url download request " + "should have a response body");
        response.responseBody().ifPresent(abortableInputStream -> {
          try {
            IoUtils.copy(abortableInputStream, bos);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
        assertEquals(CONTENT, bos.toString("UTF-8"));
      }
    }

    @Test
    public void testPresignedUrlHead() throws Exception {
      final String keyName = getKeyName();

      s3Client.putObject(b -> b.bucket(BUCKET_NAME).key(keyName), RequestBody.fromString(CONTENT));

      HeadObjectRequest objectRequest = HeadObjectRequest.builder().bucket(BUCKET_NAME).key(keyName).build();

      HeadObjectPresignRequest presignRequest = HeadObjectPresignRequest.builder()
          .signatureDuration(duration)
          .headObjectRequest(objectRequest)
          .build();

      PresignedHeadObjectRequest presignedRequest = presigner.presignHeadObject(presignRequest);

      URL presignedUrl = presignedRequest.url();
      HttpURLConnection connection = null;
      try {
        connection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "HEAD", null, null);
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "HeadObject presigned URL should return 200 OK");
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }

      // Use the AWS SDK for Java SdkHttpClient class to test the HEAD request
      SdkHttpRequest request = SdkHttpRequest.builder().method(SdkHttpMethod.HEAD).uri(presignedUrl.toURI()).build();

      HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(request).build();

      HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
      assertEquals(200, response.httpResponse().statusCode(),
          "HeadObject presigned URL should return 200 OK via SdkHttpClient");

      // Test HeadBucket presigned URL
      HeadBucketRequest bucketRequest = HeadBucketRequest.builder().bucket(BUCKET_NAME).build();

      HeadBucketPresignRequest headBucketPresignRequest = HeadBucketPresignRequest.builder()
          .signatureDuration(duration)
          .headBucketRequest(bucketRequest)
          .build();

      PresignedHeadBucketRequest presignedBucketRequest = presigner.presignHeadBucket(headBucketPresignRequest);

      URL presignedBucketUrl = presignedBucketRequest.url();
      HttpURLConnection bucketConnection = null;
      try {
        bucketConnection = S3SDKTestUtils.openHttpURLConnection(presignedBucketUrl, "HEAD", null, null);
        int bucketResponseCode = bucketConnection.getResponseCode();
        assertEquals(200, bucketResponseCode, "HeadBucket presigned URL should return 200 OK");
      } finally {
        if (bucketConnection != null) {
          bucketConnection.disconnect();
        }
      }

      // Use the AWS SDK for Java SdkHttpClient class to test the HEAD request for bucket
      SdkHttpRequest bucketSdkRequest = SdkHttpRequest.builder()
          .method(SdkHttpMethod.HEAD)
          .uri(presignedBucketUrl.toURI())
          .build();
      HttpExecuteRequest bucketExecuteRequest = HttpExecuteRequest.builder().request(bucketSdkRequest).build();

      HttpExecuteResponse bucketResponse = sdkHttpClient.prepareRequest(bucketExecuteRequest).call();
      assertEquals(200, bucketResponse.httpResponse().statusCode(),
          "HeadBucket presigned URL should return 200 OK via SdkHttpClient");
    }

    @Test
    public void testPresignedUrlPut() throws Exception {
      final String keyName = getKeyName();

      PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(BUCKET_NAME).key(keyName).build();

      PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
          .signatureDuration(duration)
          .putObjectRequest(objectRequest)
          .build();

      PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

      // use http url connection
      HttpURLConnection connection = null;
      String actualContent;
      try {
        connection = S3SDKTestUtils.openHttpURLConnection(presignedRequest.url(), "PUT",
            presignedRequest.signedHeaders(), CONTENT.getBytes(StandardCharsets.UTF_8));
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "PutObject presigned URL should return 200 OK");
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
      //verify the object was uploaded
      ResponseInputStream<GetObjectResponse> object1 = s3Client.getObject(b1 -> b1.bucket(BUCKET_NAME).key(keyName));
      actualContent = IoUtils.toUtf8String(object1);
      assertEquals(CONTENT, actualContent);

      // Use the AWS SDK for Java SdkHttpClient class to test the PUT request
      SdkHttpRequest request = SdkHttpRequest.builder()
          .method(SdkHttpMethod.PUT)
          .uri(presignedRequest.url().toURI())
          .build();

      byte[] bytes = CONTENT.getBytes(StandardCharsets.UTF_8);
      HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
          .request(request)
          .contentStreamProvider(() -> new ByteArrayInputStream(bytes))
          .build();

      HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
      assertEquals(200, response.httpResponse().statusCode(),
          "PutObject presigned URL should return 200 OK via SdkHttpClient");

      //verify the object was uploaded
      ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(keyName));
      actualContent = IoUtils.toUtf8String(object);
      assertEquals(CONTENT, actualContent);
    }

    @Test
    public void testPresignedUrlMultipartUpload(@TempDir Path tempDir) throws Exception {
      final String keyName = getKeyName();
      final Map<String, String> userMetadata = new HashMap<>();
      userMetadata.put("key1", "value1");
      userMetadata.put("key2", "value2");

      List<Tag> tags = Arrays.asList(Tag.builder().key("tag1").value("value1").build(),
          Tag.builder().key("tag2").value("value2").build());

      File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();
      createFile(multipartUploadFile, (int) (10 * MB));

      // generate create MPU presigned URL
      CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
          .bucket(BUCKET_NAME)
          .key(keyName)
          .metadata(userMetadata)
          .tagging(Tagging.builder().tagSet(tags).build())
          .build();

      CreateMultipartUploadPresignRequest createMPUPresignRequest = CreateMultipartUploadPresignRequest.builder()
          .signatureDuration(duration)
          .createMultipartUploadRequest(createRequest)
          .build();

      PresignedCreateMultipartUploadRequest presignCreateMultipartUpload =
          presigner.presignCreateMultipartUpload(createMPUPresignRequest);

      mpuWithHttpURLConnection(presignCreateMultipartUpload, multipartUploadFile, keyName, userMetadata, tags);
      mpuWithSdkHttpClient(presignCreateMultipartUpload, multipartUploadFile, keyName, userMetadata, tags);
    }

    private void mpuWithHttpURLConnection(PresignedCreateMultipartUploadRequest presignCreateMultipartUpload,
                                          File multipartUploadFile, String keyName, Map<String, String> userMetadata,
                                          List<Tag> tags) throws IOException {
      // create MPU using presigned URL
      String uploadId;
      HttpURLConnection createMPUConnection = null;
      try {
        createMPUConnection = S3SDKTestUtils.openHttpURLConnection(presignCreateMultipartUpload.url(), "POST",
            presignCreateMultipartUpload.signedHeaders(), null);
        int createMultiPartUploadConnectionResponseCode = createMPUConnection.getResponseCode();
        assertEquals(200, createMultiPartUploadConnectionResponseCode,
            "CreateMultipartUploadPresignRequest should return 200 OK");

        try (InputStream is = createMPUConnection.getInputStream()) {
          String responseXml = IOUtils.toString(is, StandardCharsets.UTF_8);
          uploadId = S3SDKTestUtils.extractUploadId(responseXml);
        }
      } finally {
        if (createMPUConnection != null) {
          createMPUConnection.disconnect();
        }
      }

      // Upload parts using presigned URL
      List<CompletedPart> completedParts = uploadPartWithHttpURLConnection(multipartUploadFile, keyName, uploadId);

      // complete MPU using presigned URL
      CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
          .bucket(BUCKET_NAME)
          .key(keyName)
          .uploadId(uploadId)
          .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
          .build();
      CompleteMultipartUploadPresignRequest completeMPUPresignRequest = CompleteMultipartUploadPresignRequest.builder()
          .signatureDuration(duration)
          .completeMultipartUploadRequest(completeRequest)
          .build();

      PresignedCompleteMultipartUploadRequest presignedCompleteMultipartUploadRequest =
          presigner.presignCompleteMultipartUpload(completeMPUPresignRequest);

      completeMPUWithHttpUrlConnection(presignedCompleteMultipartUploadRequest, completedParts);

      // verify upload result
      HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(keyName));
      assertTrue(headObjectResponse.hasMetadata());
      assertEquals(userMetadata, headObjectResponse.metadata());

      ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(keyName));
      assertEquals(tags.size(), object.response().tagCount());
      String actualContent = IoUtils.toUtf8String(object);
      String originalContent = new String(Files.readAllBytes(multipartUploadFile.toPath()), StandardCharsets.UTF_8);
      assertEquals(originalContent, actualContent, "Uploaded file content should match original file content");
    }

    private List<CompletedPart> uploadPartWithHttpURLConnection(File multipartUploadFile, String keyName,
                                                                String uploadId) throws IOException {
      List<CompletedPart> completedParts = new ArrayList<>();
      int partNumber = 1;
      ByteBuffer bb = ByteBuffer.allocate((int) (5 * MB));

      try (RandomAccessFile file = new RandomAccessFile(multipartUploadFile, "r")) {
        long fileSize = file.length();
        long position = 0;

        while (position < fileSize) {
          file.seek(position);
          long read = file.getChannel().read(bb);

          bb.flip();

          // First create an UploadPartRequest
          UploadPartRequest request = UploadPartRequest.builder()
              .bucket(BUCKET_NAME)
              .key(keyName)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentLength((long) bb.remaining())
              .build();

          // Generate presigned URL for each part
          UploadPartPresignRequest presignRequest = UploadPartPresignRequest.builder()
              .signatureDuration(duration)
              .uploadPartRequest(request)
              .build();

          PresignedUploadPartRequest presignedRequest = presigner.presignUploadPart(presignRequest);

          // use presigned URL to upload the part
          HttpURLConnection connection = null;
          try {
            byte[] body = new byte[bb.remaining()];
            bb.get(body);
            connection = S3SDKTestUtils.openHttpURLConnection(presignedRequest.url(), "PUT",
                presignedRequest.signedHeaders(), body);

            int responseCode = connection.getResponseCode();
            assertEquals(200, responseCode, String.format("Upload part %d should return 200 OK", partNumber));

            String etag = connection.getHeaderField("ETag");
            CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
            completedParts.add(part);
          } finally {
            if (connection != null) {
              connection.disconnect();
            }
          }

          bb.clear();
          position += read;
          partNumber++;
        }
      }
      return completedParts;
    }

    private void completeMPUWithHttpUrlConnection(PresignedCompleteMultipartUploadRequest request,
                                                  List<CompletedPart> completedParts) throws IOException {
      HttpURLConnection connection = null;
      try {
        String xmlPayload = buildCompleteMultipartUploadXml(completedParts);
        byte[] payloadBytes = xmlPayload.getBytes(StandardCharsets.UTF_8);
        connection = S3SDKTestUtils.openHttpURLConnection(request.url(), "POST", request.signedHeaders(), payloadBytes);

        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "CompleteMultipartUploadPresignRequest should return 200 OK");
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    private void mpuWithSdkHttpClient(PresignedCreateMultipartUploadRequest presignCreateMultipartUpload,
                                      File multipartUploadFile, String keyName, Map<String, String> userMetadata,
                                      List<Tag> tags) throws Exception {
      // create MPU using presigned URL
      String uploadId = createMPUWithSdkHttpClient(presignCreateMultipartUpload);

      // Upload parts using presigned URL
      List<CompletedPart> completedParts = uploadPartWithSdkHttpClient(multipartUploadFile, keyName, uploadId);

      // complete MPU using presigned URL
      completeMPUWithSdkHttpClient(keyName, uploadId, completedParts);

      // verify upload result
      HeadObjectResponse headObjectResponse = s3Client.headObject(b -> b.bucket(BUCKET_NAME).key(keyName));
      assertTrue(headObjectResponse.hasMetadata());
      assertEquals(userMetadata, headObjectResponse.metadata());

      ResponseInputStream<GetObjectResponse> object = s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(keyName));
      assertEquals(tags.size(), object.response().tagCount());
      String actualContent = IoUtils.toUtf8String(object);
      String originalContent = new String(Files.readAllBytes(multipartUploadFile.toPath()), StandardCharsets.UTF_8);
      assertEquals(originalContent, actualContent, "Uploaded file content should match original file content");
    }

    private void completeMPUWithSdkHttpClient(String keyName, String uploadId, List<CompletedPart> completedParts)
        throws Exception {
      CompleteMultipartUploadRequest request = CompleteMultipartUploadRequest.builder()
          .bucket(BUCKET_NAME)
          .key(keyName)
          .uploadId(uploadId)
          .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
          .build();

      CompleteMultipartUploadPresignRequest presignRequest = CompleteMultipartUploadPresignRequest.builder()
          .signatureDuration(duration)
          .completeMultipartUploadRequest(request)
          .build();

      PresignedCompleteMultipartUploadRequest presignedCompleteMultipartUploadRequest =
          presigner.presignCompleteMultipartUpload(presignRequest);

      String xmlPayload = buildCompleteMultipartUploadXml(completedParts);
      byte[] payloadBytes = xmlPayload.getBytes(StandardCharsets.UTF_8);

      SdkHttpRequest sdkHttpRequest = SdkHttpRequest.builder()
          .method(SdkHttpMethod.POST)
          .uri(presignedCompleteMultipartUploadRequest.url().toURI())
          .build();

      HttpExecuteRequest httpExecuteRequest = HttpExecuteRequest.builder()
          .request(sdkHttpRequest)
          .contentStreamProvider(() -> new ByteArrayInputStream(payloadBytes))
          .build();

      HttpExecuteResponse response = sdkHttpClient.prepareRequest(httpExecuteRequest).call();
      assertEquals(200, response.httpResponse().statusCode(),
          "CompleteMultipartUploadPresignRequest should return 200 OK");
    }

    private List<CompletedPart> uploadPartWithSdkHttpClient(File multipartUploadFile, String keyName, String uploadId)
        throws Exception {
      List<CompletedPart> completedParts = new ArrayList<>();
      int partNumber = 1;
      ByteBuffer bb = ByteBuffer.allocate((int) (5 * MB));

      try (RandomAccessFile file = new RandomAccessFile(multipartUploadFile, "r")) {
        long fileSize = file.length();
        long position = 0;

        while (position < fileSize) {
          file.seek(position);
          long read = file.getChannel().read(bb);

          bb.flip();

          // Generate presigned URL for each part
          UploadPartRequest request = UploadPartRequest.builder()
              .bucket(BUCKET_NAME)
              .key(keyName)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentLength((long) bb.remaining())
              .build();

          UploadPartPresignRequest presignRequest = UploadPartPresignRequest.builder()
              .signatureDuration(duration)
              .uploadPartRequest(request)
              .build();

          PresignedUploadPartRequest presignedRequest = presigner.presignUploadPart(presignRequest);

          // upload each part using presigned URL
          SdkHttpRequest uploadPartSdkRequest = SdkHttpRequest.builder()
              .method(SdkHttpMethod.PUT)
              .uri(presignedRequest.url().toURI())
              .build();

          byte[] bytes = new byte[bb.remaining()];
          bb.get(bytes);

          HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
              .request(uploadPartSdkRequest)
              .contentStreamProvider(() -> new ByteArrayInputStream(bytes))
              .build();

          HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();

          String etag = response.httpResponse().firstMatchingHeader("ETag")
              .orElseThrow(() -> new RuntimeException("ETag missing in response"));

          CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
          completedParts.add(part);

          bb.clear();
          position += read;
          partNumber++;
        }
      }
      return completedParts;
    }

    private String createMPUWithSdkHttpClient(PresignedCreateMultipartUploadRequest request) throws Exception {
      String uploadId;
      SdkHttpRequest sdkHttpRequest = SdkHttpRequest.builder()
          .method(SdkHttpMethod.POST)
          .uri(request.url().toURI())
          .headers(request.signedHeaders())
          .build();

      HttpExecuteRequest httpExecuteRequest = HttpExecuteRequest.builder()
          .request(sdkHttpRequest)
          .build();

      HttpExecuteResponse response = sdkHttpClient.prepareRequest(httpExecuteRequest).call();
      try (InputStream is = response.responseBody().get()) {
        String responseXml = IOUtils.toString(is, StandardCharsets.UTF_8);
        uploadId = S3SDKTestUtils.extractUploadId(responseXml);
      }
      return uploadId;
    }

    private String buildCompleteMultipartUploadXml(List<CompletedPart> parts) {
      StringBuilder xml = new StringBuilder();
      xml.append("<CompleteMultipartUpload>\n");
      for (CompletedPart part : parts) {
        xml.append("  <Part>\n");
        xml.append("    <PartNumber>").append(part.partNumber()).append("</PartNumber>\n");
        xml.append("    <ETag>").append(stripQuotes(part.eTag())).append("</ETag>\n");
        xml.append("  </Part>\n");
      }
      xml.append("</CompleteMultipartUpload>");
      return xml.toString();
    }

    @Test
    public void testPresignedUrlDelete() throws Exception {
      final String keyName = getKeyName();

      s3Client.putObject(b -> b.bucket(BUCKET_NAME).key(keyName), RequestBody.fromString(CONTENT));

      DeleteObjectRequest objectRequest = DeleteObjectRequest.builder().bucket(BUCKET_NAME).key(keyName).build();

      DeleteObjectPresignRequest presignRequest = DeleteObjectPresignRequest.builder()
          .signatureDuration(Duration.ofMinutes(10))
          .deleteObjectRequest(objectRequest)
          .build();

      PresignedDeleteObjectRequest presignedRequest = presigner.presignDeleteObject(presignRequest);

      // use http url connection
      HttpURLConnection connection = null;
      try {
        connection = S3SDKTestUtils.openHttpURLConnection(presignedRequest.url(), "DELETE", null, null);
        int responseCode = connection.getResponseCode();
        assertEquals(204, responseCode, "DeleteObject presigned URL should return 204 No Content");

        //verify the object was deleted
        assertThrows(NoSuchKeyException.class, () -> s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(keyName)));
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }

      // use SdkHttpClient
      s3Client.putObject(b -> b.bucket(BUCKET_NAME).key(keyName), RequestBody.fromString(CONTENT));

      SdkHttpRequest request = SdkHttpRequest.builder()
          .method(SdkHttpMethod.DELETE)
          .uri(presignedRequest.url().toURI())
          .build();

      HttpExecuteRequest executeRequest = HttpExecuteRequest.builder().request(request).build();

      HttpExecuteResponse response = sdkHttpClient.prepareRequest(executeRequest).call();
      assertEquals(204, response.httpResponse().statusCode(),
          "DeleteObject presigned URL should return 204 No Content via SdkHttpClient");

      //verify the object was deleted
      assertThrows(NoSuchKeyException.class, () -> s3Client.getObject(b -> b.bucket(BUCKET_NAME).key(keyName)));
    }
  }

  /**
   * Tests the functionality to create a snapshot of an Ozone bucket and then read files
   * from the snapshot directory using the S3 SDK.
   *
   * <p>The test follows these steps:
   * <ol>
   *   <li>Create a bucket and upload a file via the S3 client.</li>
   *   <li>Create a snapshot on the bucket using the Ozone client.</li>
   *   <li>Construct the snapshot object key using the ".snapshot" directory format.</li>
   *   <li>Retrieve the object from the snapshot and verify that its content matches
   *       the originally uploaded content.</li>
   * </ol>
   * </p>
   *
   * @throws Exception if the test fails due to any errors during bucket creation, snapshot creation,
   *         file upload, or retrieval.
   */
  @Test
  public void testReadSnapshotDirectoryUsingS3SDK() throws Exception {
    final String bucketName = getBucketName("snapshot");
    final String keyName = getKeyName("snapshotfile");
    final String content = "snapshot test content";

    // Create the bucket and upload an object using S3 SDK.
    s3Client.createBucket(b -> b.bucket(bucketName));
    s3Client.putObject(b -> b.bucket(bucketName).key(keyName),
        RequestBody.fromString(content));

    String snapshotName = "snap1";
    // Create a snapshot using the Ozone client.
    // Snapshots in Ozone are created on the bucket and are exposed via the ".snapshot" directory.
    try (OzoneClient ozoneClient = cluster.newClient()) {
      ObjectStore store = ozoneClient.getObjectStore();
      OzoneVolume volume = store.getS3Volume();

      store.createSnapshot(volume.getName(), bucketName, snapshotName);
    }

    // Use the S3 SDK to read the file from the snapshot directory.
    // The key in the snapshot is constructed using the special ".snapshot" prefix.
    String snapshotKey = ".snapshot/" + snapshotName + "/" + keyName;
    ResponseBytes<GetObjectResponse> snapshotResponse = s3Client.getObjectAsBytes(
        b -> b.bucket(bucketName).key(snapshotKey));

    assertEquals(content, snapshotResponse.asUtf8String());
  }

  private String getBucketName() {
    return getBucketName("");
  }

  private String getBucketName(String ignored) {
    return uniqueObjectName();
  }

  private String getKeyName() {
    return getKeyName("");
  }

  private String getKeyName(String ignored) {
    return uniqueObjectName();
  }

  private String multipartUpload(String bucketName, String key, File file, int partSize,
                                 Map<String, String> userMetadata, List<Tag> tags) throws Exception {
    String uploadId = initiateMultipartUpload(bucketName, key, userMetadata, tags);

    List<CompletedPart> completedParts = uploadParts(bucketName, key, uploadId, file, partSize);

    completeMultipartUpload(bucketName, key, uploadId, completedParts);

    return uploadId;
  }

  private String initiateMultipartUpload(String bucketName, String key,
                                         Map<String, String> metadata, List<Tag> tags) {
    CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(b -> b
        .bucket(bucketName)
        .metadata(metadata)
        .tagging(Tagging.builder().tagSet(tags).build())
        .key(key));

    assertEquals(bucketName, createMultipartUploadResponse.bucket());
    assertEquals(key, createMultipartUploadResponse.key());
    // TODO: Once bucket lifecycle configuration is supported, should check for "abortDate" and "abortRuleId"

    return createMultipartUploadResponse.uploadId();
  }

  private List<CompletedPart> uploadParts(String bucketName, String key, String uploadId, File inputFile, int partSize)
      throws Exception {
    // Upload the parts of the file
    int partNumber = 1;
    ByteBuffer bb = ByteBuffer.allocate(partSize);
    List<CompletedPart> completedParts = new ArrayList<>();
    try (RandomAccessFile file = new RandomAccessFile(inputFile, "r");
         InputStream fileInputStream = Files.newInputStream(inputFile.toPath())) {
      long fileSize = file.length();
      long position = 0;
      while (position < fileSize) {
        file.seek(position);
        long read = file.getChannel().read(bb);

        bb.flip(); // Swap position and limit before reading from the buffer
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .bucket(bucketName)
            .key(key)
            .uploadId(uploadId)
            .partNumber(partNumber)
            .build();

        UploadPartResponse partResponse = s3Client.uploadPart(
            uploadPartRequest,
            RequestBody.fromByteBuffer(bb));

        assertEquals(DatatypeConverter.printHexBinary(
            calculateDigest(fileInputStream, 0, partSize)).toLowerCase(), stripQuotes(partResponse.eTag()));

        CompletedPart part = CompletedPart.builder()
            .partNumber(partNumber)
            .eTag(stripQuotes(partResponse.eTag()))
            .build();
        completedParts.add(part);

        bb.clear();
        position += read;
        partNumber++;
      }
    }

    return completedParts;
  }

  private void completeMultipartUpload(String bucketName, String key, String uploadId,
                                       List<CompletedPart> completedParts) {
    CompleteMultipartUploadResponse compResponse = s3Client.completeMultipartUpload(b -> b
        .bucket(bucketName)
        .key(key)
        .uploadId(uploadId)
        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
    assertEquals(bucketName, compResponse.bucket());
    assertEquals(key, compResponse.key());
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class S3BucketOwnershipVerificationConditionsTests {

    private static final String DEFAULT_BUCKET_NAME = "default-bucket";
    private String correctOwner;
    private static final String WRONG_OWNER = "wrong-owner";
    private static final String BUCKET_VERIFICATION_TEST_KEY = "test-key";

    @BeforeAll
    public void setup() {
      // create bucket to verify bucket ownership
      s3Client.createBucket(b -> b.bucket(DEFAULT_BUCKET_NAME));
      GetBucketAclRequest normalRequest = GetBucketAclRequest.builder().bucket(DEFAULT_BUCKET_NAME).build();
      correctOwner = s3Client.getBucketAcl(normalRequest).owner().displayName();

      // create objects to verify bucket ownership
      s3Client.putObject(b -> b.bucket(DEFAULT_BUCKET_NAME).key(BUCKET_VERIFICATION_TEST_KEY), RequestBody.empty());
    }

    @Nested
    class BucketEndpointTests {

      @Test
      public void testGetBucketAcl() {
        GetBucketAclRequest correctRequest = GetBucketAclRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.getBucketAcl(correctRequest));

        GetBucketAclRequest wrongRequest = GetBucketAclRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getBucketAcl(wrongRequest));
      }

      @Test
      public void testListObjects() {
        ListObjectsRequest correctRequest = ListObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.listObjects(correctRequest));

        ListObjectsRequest wrongRequest = ListObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.listObjects(wrongRequest));
      }

      @Test
      public void testListMultipartUploads() {
        ListMultipartUploadsRequest correctRequest = ListMultipartUploadsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .maxUploads(5000)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.listMultipartUploads(correctRequest));

        ListMultipartUploadsRequest wrongRequest = ListMultipartUploadsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(
            () -> s3Client.listMultipartUploads(wrongRequest));
      }

      @Test
      public void testPutBucketAcl() {
        PutBucketAclRequest correctRequest = PutBucketAclRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .grantRead("")
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.putBucketAcl(correctRequest));

        PutBucketAclRequest wrongRequest = PutBucketAclRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .grantRead("")
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putBucketAcl(wrongRequest));
      }

      @Test
      public void testHeadBucket() {
        HeadBucketRequest correctRequest = HeadBucketRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.headBucket(correctRequest));

        HeadBucketRequest wrongRequest = HeadBucketRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        S3Exception exception = assertThrows(S3Exception.class, () -> s3Client.headBucket(wrongRequest));
        assertEquals(HTTP_FORBIDDEN, exception.statusCode());
      }

      @Test
      public void testDeleteBucket() {
        s3Client.createBucket(builder -> builder.bucket("for-delete"));
        String newCorrectOwner = s3Client.getBucketAcl(builder -> builder.bucket("for-delete")).owner().displayName();

        DeleteBucketRequest wrongRequest = DeleteBucketRequest.builder()
            .bucket("for-delete")
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteBucket(wrongRequest));

        DeleteBucketRequest correctRequest = DeleteBucketRequest.builder()
            .bucket("for-delete")
            .expectedBucketOwner(newCorrectOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.deleteBucket(correctRequest));
      }

      @Test
      public void testMultiDelete() {
        DeleteObjectsRequest correctRequest = DeleteObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .delete(Delete.builder().objects(ObjectIdentifier.builder().key("test").build()).build())
            .build();

        verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjects(correctRequest));

        DeleteObjectsRequest wrongRequest = DeleteObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .delete(Delete.builder().objects(ObjectIdentifier.builder().key("test").build()).build())
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjects(wrongRequest));
      }
    }

    @Nested
    class ObjectEndpointTests {

      private static final String TEST_CONTENT = "test-content";

      @Test
      public void testCreateKey() {
        String newKey = "create-key";

        PutObjectRequest wrongRequest = PutObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObject(wrongRequest, fromString(TEST_CONTENT)));

        PutObjectRequest correctRequest = PutObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.putObject(correctRequest, fromString(TEST_CONTENT)));
      }

      @Test
      public void testPutObjectTagging() {
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.builder().key("env").value("test").build());
        tags.add(Tag.builder().key("project").value("example").build());
        PutObjectTaggingRequest wrongRequest = PutObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .tagging(Tagging.builder().tagSet(tags).build())
            .expectedBucketOwner(WRONG_OWNER)
            .build();

        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObjectTagging(wrongRequest));

        PutObjectTaggingRequest correctRequest = PutObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .tagging(Tagging.builder().tagSet(tags).build())
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.putObjectTagging(correctRequest));
      }

      @Test
      public void testCreateMultipartKey() {
        CreateMultipartUploadRequest wrongRequest = CreateMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.createMultipartUpload(wrongRequest));

        CreateMultipartUploadRequest correctRequest = CreateMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.createMultipartUpload(correctRequest));
      }

      @Test
      public void testCreateMultipartByCopy() {
        String sourceKey = "test-multipart-by-copy-source-key";
        String destKey = "test-multipart-by-copy-dest-key";

        s3Client.putObject(b -> b.bucket(DEFAULT_BUCKET_NAME).key(sourceKey), fromString(TEST_CONTENT));

        CreateMultipartUploadResponse initResponse =
            s3Client.createMultipartUpload(b -> b.bucket(DEFAULT_BUCKET_NAME).key(destKey));

        String uploadId = initResponse.uploadId();

        UploadPartCopyRequest wrongRequest = UploadPartCopyRequest.builder()
            .sourceBucket(DEFAULT_BUCKET_NAME)
            .sourceKey(sourceKey)
            .expectedSourceBucketOwner(WRONG_OWNER)
            .destinationBucket(DEFAULT_BUCKET_NAME)
            .destinationKey(destKey)
            .uploadId(uploadId)
            .partNumber(1)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.uploadPartCopy(wrongRequest));

        UploadPartCopyRequest correctRequest = UploadPartCopyRequest.builder()
            .sourceBucket(DEFAULT_BUCKET_NAME)
            .sourceKey(sourceKey)
            .expectedSourceBucketOwner(correctOwner)
            .destinationBucket(DEFAULT_BUCKET_NAME)
            .destinationKey(destKey)
            .uploadId(uploadId)
            .expectedBucketOwner(correctOwner)
            .partNumber(1)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.uploadPartCopy(correctRequest));
      }

      @Test
      public void testCopyObject() {
        String sourceKey = "test-copy-object-source-key";
        String destKey = "test-copy-object-dest-key";
        s3Client.putObject(b -> b.bucket(DEFAULT_BUCKET_NAME).key(sourceKey), fromString("test"));

        CopyObjectRequest wrongRequest = CopyObjectRequest.builder()
            .sourceBucket(DEFAULT_BUCKET_NAME)
            .sourceKey(sourceKey)
            .destinationBucket(DEFAULT_BUCKET_NAME)
            .destinationKey(destKey)
            .expectedSourceBucketOwner(WRONG_OWNER)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.copyObject(wrongRequest));

        CopyObjectRequest correctRequest = CopyObjectRequest.builder()
            .sourceBucket(DEFAULT_BUCKET_NAME)
            .sourceKey(sourceKey)
            .destinationBucket(DEFAULT_BUCKET_NAME)
            .destinationKey(destKey)
            .expectedSourceBucketOwner(correctOwner)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.copyObject(correctRequest));
      }

      @Test
      public void testCreateDirectory() {
        String newKey = "create-directory-key/";

        PutObjectRequest wrongRequest = PutObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.putObject(wrongRequest, RequestBody.empty()));

        PutObjectRequest correctRequest = PutObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.putObject(correctRequest, RequestBody.empty()));
      }

      @Test
      public void testGetKey() {
        GetObjectRequest correctRequest = GetObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.getObject(correctRequest));

        GetObjectRequest wrongRequest = GetObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getObject(wrongRequest));
      }

      @Test
      public void testGetObjectTagging() {
        GetObjectTaggingRequest correctRequest = GetObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.getObjectTagging(correctRequest));

        GetObjectTaggingRequest wrongRequest = GetObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getObjectTagging(wrongRequest));
      }

      @Test
      public void testListParts() {
        String newKey = "list-parts-key";
        CreateMultipartUploadResponse multipartUploadResponse = s3Client.createMultipartUpload(b -> {
          b.bucket(DEFAULT_BUCKET_NAME)
              .key(newKey)
              .build();
        });

        String uploadId = multipartUploadResponse.uploadId();

        s3Client.uploadPart(
            UploadPartRequest.builder()
                .bucket(DEFAULT_BUCKET_NAME)
                .key(newKey)
                .uploadId(uploadId)
                .partNumber(1)
                .contentLength((long) TEST_CONTENT.getBytes(StandardCharsets.UTF_8).length)
                .build(), fromString(TEST_CONTENT));

        ListPartsRequest correctRequest = ListPartsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .uploadId(uploadId)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.listParts(correctRequest));

        ListPartsRequest wrongResponse = ListPartsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .uploadId(uploadId)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.listParts(wrongResponse));
      }

      @Test
      public void testHeadKey() {
        HeadObjectRequest correctRequest = HeadObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.headObject(correctRequest));

        HeadObjectRequest wrongRequest = HeadObjectRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        S3Exception exception = assertThrows(S3Exception.class, () -> s3Client.headObject(wrongRequest));
        assertEquals(HTTP_FORBIDDEN, exception.statusCode());
      }

      @Test
      public void testDeleteKey() {
        String newKey = "delete-key";
        s3Client.putObject(b -> b.bucket(DEFAULT_BUCKET_NAME).key(newKey), fromString(TEST_CONTENT));

        DeleteObjectsRequest wrongRequest = DeleteObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(WRONG_OWNER)
            .delete(Delete.builder().objects(ObjectIdentifier.builder().key(newKey).build()).build())
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjects(wrongRequest));

        DeleteObjectsRequest correctRequest = DeleteObjectsRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .expectedBucketOwner(correctOwner)
            .delete(Delete.builder().objects(ObjectIdentifier.builder().key(newKey).build()).build())
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjects(correctRequest));
      }

      @Test
      public void testDeleteObjectTagging() {
        DeleteObjectTaggingRequest correctRequest = DeleteObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.deleteObjectTagging(correctRequest));

        DeleteObjectTaggingRequest wrongRequest = DeleteObjectTaggingRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.deleteObjectTagging(wrongRequest));
      }

      @Test
      public void testAbortMultipartUpload() {
        CreateMultipartUploadResponse multipartUploadResponse =
            s3Client.createMultipartUpload(b -> b.bucket(DEFAULT_BUCKET_NAME).key(BUCKET_VERIFICATION_TEST_KEY));

        String uploadId = multipartUploadResponse.uploadId();

        AbortMultipartUploadRequest wrongRequest = AbortMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .uploadId(uploadId)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.abortMultipartUpload(wrongRequest));

        AbortMultipartUploadRequest correctRequest = AbortMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(BUCKET_VERIFICATION_TEST_KEY)
            .uploadId(uploadId)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.abortMultipartUpload(correctRequest));
      }

      @Test
      public void testInitMultipartUpload() {
        String newKey = "init-multipart-upload-key";

        CreateMultipartUploadRequest wrongRequest = CreateMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.createMultipartUpload(wrongRequest));

        CreateMultipartUploadRequest correctRequest = CreateMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.createMultipartUpload(correctRequest));
      }

      @Test
      public void testCompleteMultipartUpload() {
        String newKey = "complete-multipart-upload-key";
        CreateMultipartUploadResponse multipartUploadResponse =
            s3Client.createMultipartUpload(b -> b.bucket(DEFAULT_BUCKET_NAME).key(newKey));

        String uploadId = multipartUploadResponse.uploadId();

        UploadPartResponse uploadPartResponse = s3Client.uploadPart(b -> b.bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .uploadId(uploadId)
            .partNumber(1)
            .contentLength((long) TEST_CONTENT.getBytes(StandardCharsets.UTF_8).length)
            .build(), fromString(TEST_CONTENT));

        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
            .parts(
                CompletedPart.builder().partNumber(1).eTag(stripQuotes(uploadPartResponse.eTag())).build()
            ).build();


        CompleteMultipartUploadRequest wrongRequest = CompleteMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            .expectedBucketOwner(WRONG_OWNER)
            .build();
        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.completeMultipartUpload(wrongRequest));

        CompleteMultipartUploadRequest correctRequest = CompleteMultipartUploadRequest.builder()
            .bucket(DEFAULT_BUCKET_NAME)
            .key(newKey)
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            .expectedBucketOwner(correctOwner)
            .build();
        verifyPassBucketOwnershipVerification(() -> s3Client.completeMultipartUpload(correctRequest));
      }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class LinkBucketTests {
      private String nonS3VolumeName;
      private String linkBucketName;
      private String danglingSourceBucketName;
      private String danglingLinkBucketName;
      private OzoneVolume nonS3Volume;
      private OzoneVolume s3Volume;

      @BeforeAll
      public void setup() throws Exception {
        try (OzoneClient ozoneClient = cluster.newClient()) {
          nonS3VolumeName = randomName("link-vol");
          linkBucketName = randomName("link-bucket");
          danglingSourceBucketName = randomName("link-source");
          danglingLinkBucketName = randomName("link-bucket-dangling");
          ozoneClient.getObjectStore().createVolume(nonS3VolumeName);
          nonS3Volume = ozoneClient.getObjectStore().getVolume(nonS3VolumeName);
          s3Volume = ozoneClient.getObjectStore().getS3Volume();
        }
      }

      @Test
      public void setBucketVerificationOnLinkBucket() throws Exception {
        String sourceBucketName = randomName("source");
        nonS3Volume.createBucket(sourceBucketName);
        BucketArgs.Builder bb = new BucketArgs.Builder()
            .setStorageType(StorageType.DEFAULT)
            .setVersioning(false)
            .setSourceVolume(nonS3VolumeName)
            .setSourceBucket(sourceBucketName);
        s3Volume.createBucket(linkBucketName, bb.build());

        GetBucketAclRequest wrongRequest = GetBucketAclRequest.builder()
            .bucket(linkBucketName)
            .expectedBucketOwner(WRONG_OWNER)
            .build();

        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getBucketAcl(wrongRequest));

        String owner = s3Client.getBucketAcl(GetBucketAclRequest.builder()
            .bucket(linkBucketName)
            .build()).owner().displayName();
        GetBucketAclRequest correctRequest = GetBucketAclRequest.builder()
            .bucket(linkBucketName)
            .expectedBucketOwner(owner)
            .build();

        verifyPassBucketOwnershipVerification(() -> s3Client.getBucketAcl(correctRequest));
      }

      @Test
      public void testDanglingBucket() throws Exception {
        nonS3Volume.createBucket(danglingSourceBucketName);
        BucketArgs.Builder bb = new BucketArgs.Builder()
            .setStorageType(StorageType.DEFAULT)
            .setVersioning(false)
            .setSourceVolume(nonS3VolumeName)
            .setSourceBucket(danglingSourceBucketName);
        s3Volume.createBucket(danglingLinkBucketName, bb.build());

        // remove source bucket to make dangling bucket
        nonS3Volume.deleteBucket(danglingSourceBucketName);

        GetBucketAclRequest wrongRequest = GetBucketAclRequest.builder()
            .bucket(danglingLinkBucketName)
            .expectedBucketOwner(WRONG_OWNER)
            .build();

        verifyBucketOwnershipVerificationAccessDenied(() -> s3Client.getBucketAcl(wrongRequest));

        String owner = s3Client.getBucketAcl(GetBucketAclRequest.builder()
            .bucket(danglingLinkBucketName)
            .build()).owner().displayName();
        GetBucketAclRequest correctRequest = GetBucketAclRequest.builder()
            .bucket(danglingLinkBucketName)
            .expectedBucketOwner(owner)
            .build();

        verifyPassBucketOwnershipVerification(() -> s3Client.getBucketAcl(correctRequest));
      }

      private String randomName(String prefix) {
        return (prefix + "-" + RandomStringUtils.secure().nextAlphanumeric(8))
            .toLowerCase(Locale.ROOT);
      }
    }

    private void verifyPassBucketOwnershipVerification(Executable function) {
      assertDoesNotThrow(function);
    }

    private void verifyBucketOwnershipVerificationAccessDenied(Executable function) {
      S3Exception exception = assertThrows(S3Exception.class, function);
      assertEquals(HTTP_FORBIDDEN, exception.statusCode());
      assertEquals("Access Denied", exception.awsErrorDetails().errorCode());
    }
  }
}
