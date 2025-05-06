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

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.calculateDigest;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.S3GatewayService;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

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
public abstract class AbstractS3SDKV2Tests extends OzoneTestBase {

  private static MiniOzoneCluster cluster = null;
  private static S3Client s3Client = null;

  /**
   * Create a MiniOzoneCluster with S3G enabled for testing.
   * @param conf Configurations to start the cluster
   * @throws Exception exception thrown when waiting for the cluster to be ready.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    S3GatewayService s3g = new S3GatewayService();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .addService(s3g)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    s3Client = new S3ClientFactory(s3g.getConf()).createS3ClientV2();
  }

  /**
   * Shutdown the MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
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

  private String getBucketName() {
    return getBucketName("");
  }

  private String getBucketName(String suffix) {
    return (getTestName() + "bucket" + suffix).toLowerCase(Locale.ROOT);
  }

  private String getKeyName() {
    return getKeyName("");
  }

  private String getKeyName(String suffix) {
    return (getTestName() +  "key" + suffix).toLowerCase(Locale.ROOT);
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
            calculateDigest(fileInputStream, 0, partSize)).toLowerCase(), partResponse.eTag());

        CompletedPart part = CompletedPart.builder()
            .partNumber(partNumber)
            .eTag(partResponse.eTag())
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
}
