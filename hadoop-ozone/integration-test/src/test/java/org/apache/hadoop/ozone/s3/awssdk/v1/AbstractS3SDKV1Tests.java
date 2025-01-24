/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.s3.awssdk.v1;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grantee;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.utils.InputSubstream;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from
 * - https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/java/example_code/s3/
 * - https://github.com/ceph/s3-tests
 *
 * TODO: Currently we are using AWS SDK V1, need to also add tests for AWS SDK V2.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
public abstract class AbstractS3SDKV1Tests extends OzoneTestBase {

  /**
   * There are still some unsupported S3 operations.
   * Current unsupported S3 operations (non-exhaustive):
   * - Cross Region Replication (CrossRegionReplication.java)
   * - Versioned enabled buckets
   *   - DeleteObjectVersionEnabledBucket.java
   *   - DeleteMultipleObjectsVersionEnabledBucket.java
   *   - ListKeysVersioningEnabledBucket.java
   * - Website configurations
   *   - WebsiteConfiguration.java
   *   - SetWebsiteConfiguration.java
   *   - GetWebsiteConfiguration.java
   *   - DeleteWebsiteConfiguration.java
   * - S3 Event Notifications
   *   - EnableNotificationOnABucket.java
   * - Object tags
   *   - GetObjectTags.java
   *   - GetObjectTags2.java
   * - Bucket policy
   *   - SetBucketPolicy.java
   *   - GetBucketPolicy.java
   *   - DeleteBucketPolicy.java
   * - Bucket lifecycle configuration
   *   - LifecycleConfiguration.java
   * - Canned Bucket ACL
   *   - CreateBucketWithACL.java
   * - Object ACL
   *   - SetAcl.java
   *   - ModifyACLExistingObject.java
   *   - GetAcl.java
   * - S3 Encryption
   *   - S3Encrypt.java
   *   - S3EncryptV2.java
   * - Client-side encryption
   *   - S3ClientSideEncryptionAsymmetricMasterKey.java
   *   - S3ClientSideEncryptionSymMasterKey.java
   * - Server-side encryption
   *   - SpecifyServerSideEncryption.ajva
   *   - ServerSideEncryptionCopyObjectUsingHLWithSSEC.java
   *   - ServerSideEncryptionUsingClientSideEncryptionKey.java
   * - Dual stack endpoints
   *   - DualStackEndpoints.java
   * - Transfer acceleration
   *   - TransferAcceleration.java
   * - Temp credentials
   *   - MakingRequestsWithFederatedTempCredentials.java
   *   - MakingRequestsWithIAMTempCredentials.java
   * - Object archival
   *   - RestoreArchivedObject
   * - KMS key
   *   - UploadObjectKMSKey.java
   */

  private static MiniOzoneCluster cluster = null;
  private static AmazonS3 s3Client = null;

  /**
   * Create a MiniOzoneCluster with S3G enabled for testing.
   * @param conf Configurations to start the cluster
   * @throws Exception exception thrown when waiting for the cluster to be ready.
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .includeS3G(true)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    s3Client = cluster.newS3Client();
  }

  /**
   * Shutdown the MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static void setCluster(MiniOzoneCluster cluster) {
    AbstractS3SDKV1Tests.cluster = cluster;
  }

  public static MiniOzoneCluster getCluster() {
    return AbstractS3SDKV1Tests.cluster;
  }

  @Test
  public void testCreateBucket() {
    final String bucketName = getBucketName();

    Bucket b = s3Client.createBucket(bucketName);

    assertEquals(bucketName, b.getName());
    assertTrue(s3Client.doesBucketExist(bucketName));
    assertTrue(s3Client.doesBucketExistV2(bucketName));
    assertTrue(isBucketEmpty(b));
  }

  @Test
  public void testBucketACLOperations() {
    // TODO HDDS-11738: Uncomment assertions when bucket S3 ACL logic has been fixed
    final String bucketName = getBucketName();

    AccessControlList aclList = new AccessControlList();
    Owner owner = new Owner("owner", "owner");
    aclList.withOwner(owner);
    Grantee grantee = new CanonicalGrantee("testGrantee");
    aclList.grantPermission(grantee, Permission.Read);


    CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName)
        .withAccessControlList(aclList);

    s3Client.createBucket(createBucketRequest);

    //assertEquals(aclList, s3Client.getBucketAcl(bucketName));

    aclList.grantPermission(grantee, Permission.Write);
    s3Client.setBucketAcl(bucketName, aclList);

    //assertEquals(aclList, s3Client.getBucketAcl(bucketName));
  }

  @Test
  public void testListBuckets() {
    List<String> bucketNames = new ArrayList<>();
    for (int i = 0; i <= 5; i++) {
      String bucketName = getBucketName(String.valueOf(i));
      s3Client.createBucket(bucketName);
      bucketNames.add(bucketName);
    }

    List<Bucket> bucketList = s3Client.listBuckets();
    List<String> listBucketNames = bucketList.stream()
        .map(Bucket::getName).collect(Collectors.toList());

    assertThat(listBucketNames).containsAll(bucketNames);
  }

  @Test
  public void testDeleteBucket() {
    final String bucketName = getBucketName();

    s3Client.createBucket(bucketName);

    s3Client.deleteBucket(bucketName);

    assertFalse(s3Client.doesBucketExist(bucketName));
    assertFalse(s3Client.doesBucketExistV2(bucketName));
  }

  @Test
  public void testDeleteBucketNotExist() {
    final String bucketName = getBucketName();

    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.deleteBucket(bucketName));

    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(404, ase.getStatusCode());
    assertEquals("NoSuchBucket", ase.getErrorCode());
  }

  @Test
  public void testDeleteBucketNonEmptyWithKeys() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);

    // Upload some objects to the bucket
    for (int i = 1; i <= 10; i++) {
      s3Client.putObject(bucketName, "key-" + i, RandomStringUtils.randomAlphanumeric(1024));
    }

    // Bucket deletion should fail if there are still keys in the bucket
    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.deleteBucket(bucketName)
    );
    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(409, ase.getStatusCode());
    assertEquals("BucketNotEmpty", ase.getErrorCode());

    // Delete all the keys
    ObjectListing objectListing = s3Client.listObjects(bucketName);
    while (true) {
      for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
        s3Client.deleteObject(bucketName, summary.getKey());
      }

      // more object_listing to retrieve?
      if (objectListing.isTruncated()) {
        objectListing = s3Client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
  }

  @Test
  public void testDeleteBucketNonEmptyWithIncompleteMultipartUpload(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    s3Client.createBucket(bucketName);

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (5 * MB));

    // Create an incomplete multipart upload by initiating multipart upload,
    // uploading some parts, but not actually completing it.
    String uploadId = initiateMultipartUpload(bucketName, keyName, null, null, null);

    uploadParts(bucketName, keyName, uploadId, multipartUploadFile, 1 * MB);

    // Bucket deletion should fail if there are still keys in the bucket
    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.deleteBucket(bucketName)
    );
    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(409, ase.getStatusCode());
    assertEquals("BucketNotEmpty", ase.getErrorCode());

    // After the multipart upload is aborted, the bucket deletion should succeed
    abortMultipartUpload(bucketName, keyName, uploadId);

    s3Client.deleteBucket(bucketName);

    assertFalse(s3Client.doesBucketExistV2(bucketName));
  }

  @Test
  public void testPutObject() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    s3Client.createBucket(bucketName);

    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    PutObjectResult putObjectResult = s3Client.putObject(bucketName, keyName, is, new ObjectMetadata());
    assertEquals("37b51d194a7513e45b56f6524f2d51f2", putObjectResult.getETag());
  }

  @Test
  public void testPutObjectEmpty() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "";
    s3Client.createBucket(bucketName);

    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    PutObjectResult putObjectResult = s3Client.putObject(bucketName, keyName, is, new ObjectMetadata());
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", putObjectResult.getETag());
  }

  @Test
  public void testPutObjectACL() throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    s3Client.createBucket(bucketName);

    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    PutObjectResult putObjectResult = s3Client.putObject(bucketName, keyName, is, new ObjectMetadata());
    String originalObjectETag = putObjectResult.getETag();
    assertTrue(s3Client.doesObjectExist(bucketName, keyName));

    AccessControlList aclList = new AccessControlList();
    Owner owner = new Owner("owner", "owner");
    aclList.withOwner(owner);
    Grantee grantee = new CanonicalGrantee("testGrantee");
    aclList.grantPermission(grantee, Permission.Read);

    SetObjectAclRequest setObjectAclRequest = new SetObjectAclRequest(bucketName, keyName, aclList);

    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.setObjectAcl(setObjectAclRequest));
    assertEquals("NotImplemented", ase.getErrorCode());
    assertEquals(501, ase.getStatusCode());
    assertEquals(ErrorType.Service, ase.getErrorType());

    // Ensure that the object content remains unchanged
    ObjectMetadata updatedObjectMetadata = s3Client.getObjectMetadata(bucketName, keyName);
    assertEquals(originalObjectETag, updatedObjectMetadata.getETag());
    S3Object updatedObject = s3Client.getObject(bucketName, keyName);

    try (S3ObjectInputStream s3is = updatedObject.getObjectContent();
         ByteArrayOutputStream bos = new ByteArrayOutputStream(contentBytes.length)) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = s3is.read(readBuf)) > 0) {
        bos.write(readBuf, 0, readLen);
      }
      assertEquals(content, bos.toString("UTF-8"));
    }
  }

  @Test
  public void testGetObject() throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final String content = "bar";
    final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
    s3Client.createBucket(bucketName);

    InputStream is = new ByteArrayInputStream(contentBytes);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("key1", "value1");
    userMetadata.put("key2", "value2");
    objectMetadata.setUserMetadata(userMetadata);

    List<Tag> tags = Arrays.asList(new Tag("tag1", "value1"), new Tag("tag2", "value2"));
    ObjectTagging objectTagging = new ObjectTagging(tags);


    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, keyName, is, objectMetadata)
        .withTagging(objectTagging);

    s3Client.putObject(putObjectRequest);

    S3Object s3Object = s3Client.getObject(bucketName, keyName);
    assertEquals(tags.size(), s3Object.getTaggingCount());

    try (S3ObjectInputStream s3is = s3Object.getObjectContent();
         ByteArrayOutputStream bos = new ByteArrayOutputStream(contentBytes.length)) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = s3is.read(readBuf)) > 0) {
        bos.write(readBuf, 0, readLen);
      }
      assertEquals(content, bos.toString("UTF-8"));
    }
  }

  @Test
  public void testGetObjectWithoutETag() throws Exception {
    // Object uploaded using other protocols (e.g. ofs / ozone cli) will not
    // have ETag. Ensure that ETag will not do ETag validation on GetObject if there
    // is no ETag present.
    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    String value = "sample value";
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

    OzoneConfiguration conf = cluster.getConf();
    try (OzoneClient ozoneClient = OzoneClientFactory.getRpcClient(conf)) {
      ObjectStore store = ozoneClient.getObjectStore();

      OzoneVolume volume = store.getS3Volume();
      OzoneBucket bucket = volume.getBucket(bucketName);

      try (OzoneOutputStream out = bucket.createKey(keyName,
          valueBytes.length,
          ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
          Collections.emptyMap())) {
        out.write(valueBytes);
      }
    }

    S3Object s3Object = s3Client.getObject(bucketName, keyName);
    assertNull(s3Object.getObjectMetadata().getETag());

    try (S3ObjectInputStream s3is = s3Object.getObjectContent();
         ByteArrayOutputStream bos = new ByteArrayOutputStream(valueBytes.length)) {
      byte[] readBuf = new byte[1024];
      int readLen = 0;
      while ((readLen = s3is.read(readBuf)) > 0) {
        bos.write(readBuf, 0, readLen);
      }
      assertEquals(value, bos.toString("UTF-8"));
    }
  }

  @Test
  public void testListObjectsMany() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);
    final List<String> keyNames = Arrays.asList(
        getKeyName("1"),
        getKeyName("2"),
        getKeyName("3")
    );

    for (String keyName: keyNames) {
      s3Client.putObject(bucketName, keyName, RandomStringUtils.randomAlphanumeric(5));
    }

    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withMaxKeys(2);
    ObjectListing listObjectsResponse = s3Client.listObjects(listObjectsRequest);
    assertThat(listObjectsResponse.getObjectSummaries()).hasSize(2);
    assertEquals(bucketName, listObjectsResponse.getBucketName());
    assertEquals(listObjectsResponse.getObjectSummaries().stream()
        .map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        keyNames.subList(0, 2));
    assertTrue(listObjectsResponse.isTruncated());


    listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName)
        .withMaxKeys(2)
        .withMarker(listObjectsResponse.getNextMarker());
    listObjectsResponse = s3Client.listObjects(listObjectsRequest);
    assertThat(listObjectsResponse.getObjectSummaries()).hasSize(1);
    assertEquals(bucketName, listObjectsResponse.getBucketName());
    assertEquals(listObjectsResponse.getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        keyNames.subList(2, keyNames.size()));
    assertFalse(listObjectsResponse.isTruncated());
  }

  @Test
  public void testListObjectsManyV2() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);
    final List<String> keyNames = Arrays.asList(
        getKeyName("1"),
        getKeyName("2"),
        getKeyName("3")
    );

    for (String keyName: keyNames) {
      s3Client.putObject(bucketName, keyName, RandomStringUtils.randomAlphanumeric(5));
    }

    ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(bucketName)
        .withMaxKeys(2);
    ListObjectsV2Result listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
    assertThat(listObjectsResponse.getObjectSummaries()).hasSize(2);
    assertEquals(bucketName, listObjectsResponse.getBucketName());
    assertEquals(listObjectsResponse.getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        keyNames.subList(0, 2));
    assertTrue(listObjectsResponse.isTruncated());


    listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(bucketName)
        .withMaxKeys(2)
        .withContinuationToken(listObjectsResponse.getNextContinuationToken());
    listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
    assertThat(listObjectsResponse.getObjectSummaries()).hasSize(1);
    assertEquals(bucketName, listObjectsResponse.getBucketName());
    assertEquals(listObjectsResponse.getObjectSummaries().stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        keyNames.subList(2, keyNames.size()));
    assertFalse(listObjectsResponse.isTruncated());
  }

  @Test
  public void testListObjectsBucketNotExist() {
    final String bucketName = getBucketName();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucketName);
    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.listObjects(listObjectsRequest));
    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(404, ase.getStatusCode());
    assertEquals("NoSuchBucket", ase.getErrorCode());
  }

  @Test
  public void testListObjectsV2BucketNotExist() {
    final String bucketName = getBucketName();
    ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(bucketName);
    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.listObjectsV2(listObjectsRequest));
    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(404, ase.getStatusCode());
    assertEquals("NoSuchBucket", ase.getErrorCode());
  }

  @Test
  public void testHighLevelMultipartUpload(@TempDir Path tempDir) throws Exception {
    TransferManager tm = TransferManagerBuilder.standard()
        .withS3Client(s3Client)
        .build();

    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    // The minimum file size to for TransferManager to initiate multipart upload is 16MB, so create a file
    // larger than the threshold.
    // See TransferManagerConfiguration#getMultipartUploadThreshold
    int fileSize = (int) (20 * MB);
    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, fileSize);

    // TransferManager processes all transfers asynchronously,
    // so this call returns immediately.
    Upload upload = tm.upload(bucketName, keyName, multipartUploadFile);

    upload.waitForCompletion();
    UploadResult uploadResult = upload.waitForUploadResult();
    assertEquals(bucketName, uploadResult.getBucketName());
    assertEquals(keyName, uploadResult.getKey());
  }

  @Test
  public void testLowLevelMultipartUpload(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put("key1", "value1");
    userMetadata.put("key2", "value2");

    List<Tag> tags = Arrays.asList(new Tag("tag1", "value1"), new Tag("tag2", "value2"));

    s3Client.createBucket(bucketName);

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (25 * MB));

    multipartUpload(bucketName, keyName, multipartUploadFile, 5 * MB, null, userMetadata, tags);

    S3Object s3Object = s3Client.getObject(bucketName, keyName);
    assertEquals(keyName, s3Object.getKey());
    assertEquals(bucketName, s3Object.getBucketName());
    assertEquals(tags.size(), s3Object.getTaggingCount());

    ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, keyName);
    assertEquals(userMetadata, objectMetadata.getUserMetadata());
  }

  @Test
  public void testListMultipartUploads() {
    final String bucketName = getBucketName();
    final String multipartKey1 = getKeyName("multipart1");
    final String multipartKey2 = getKeyName("multipart2");

    s3Client.createBucket(bucketName);

    List<String> uploadIds = new ArrayList<>();

    String uploadId1 = initiateMultipartUpload(bucketName, multipartKey1, null, null, null);
    uploadIds.add(uploadId1);
    String uploadId2 = initiateMultipartUpload(bucketName, multipartKey1, null, null, null);
    uploadIds.add(uploadId2);
    // TODO: Currently, Ozone sorts based on uploadId instead of MPU init time within the same key.
    //  Remove this sorting step once HDDS-11532 has been implemented
    Collections.sort(uploadIds);
    String uploadId3 = initiateMultipartUpload(bucketName, multipartKey2, null, null, null);
    uploadIds.add(uploadId3);

    // TODO: Add test for max uploads threshold and marker once HDDS-11530 has been implemented
    ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);

    MultipartUploadListing result = s3Client.listMultipartUploads(listMultipartUploadsRequest);

    List<String> listUploadIds = result.getMultipartUploads().stream()
        .map(MultipartUpload::getUploadId)
        .collect(Collectors.toList());

    assertEquals(uploadIds, listUploadIds);
  }

  @Test
  public void testListParts(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();
    final long fileSize = 5 * MB;
    final long partSize = 1 * MB;
    final int maxParts = 2;

    s3Client.createBucket(bucketName);

    String uploadId = initiateMultipartUpload(bucketName, keyName, null, null, null);

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) fileSize);

    List<PartETag> partETags = uploadParts(bucketName, keyName, uploadId, multipartUploadFile, partSize);

    List<PartETag> listPartETags = new ArrayList<>();
    int partNumberMarker = 0;
    int expectedNumOfParts = 5;
    PartListing listPartsResult;
    do {
      ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, keyName, uploadId)
          .withMaxParts(maxParts)
          .withPartNumberMarker(partNumberMarker);
      listPartsResult = s3Client.listParts(listPartsRequest);
      if (expectedNumOfParts > maxParts) {
        assertTrue(listPartsResult.isTruncated());
        partNumberMarker = listPartsResult.getNextPartNumberMarker();
        expectedNumOfParts -= maxParts;
      } else {
        assertFalse(listPartsResult.isTruncated());
      }
      for (PartSummary partSummary : listPartsResult.getParts()) {
        listPartETags.add(new PartETag(partSummary.getPartNumber(), partSummary.getETag()));
      }
    } while (listPartsResult.isTruncated());

    assertEquals(partETags.size(), listPartETags.size());
    for (int i = 0; i < partETags.size(); i++) {
      assertEquals(partETags.get(i).getPartNumber(), listPartETags.get(i).getPartNumber());
      assertEquals(partETags.get(i).getETag(), listPartETags.get(i).getETag());
    }
  }

  @Test
  public void testGetParticularPart(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (15 * MB));

    multipartUpload(bucketName, keyName, multipartUploadFile, 5 * MB, null, null, null);

    GetObjectRequest getObjectRequestAll = new GetObjectRequest(bucketName, keyName);
    getObjectRequestAll.setPartNumber(0);
    S3Object s3ObjectAll = s3Client.getObject(getObjectRequestAll);
    long allPartContentLength = s3ObjectAll.getObjectMetadata().getContentLength();

    GetObjectRequest getObjectRequestOne = new GetObjectRequest(bucketName, keyName);
    getObjectRequestOne.setPartNumber(1);
    S3Object s3ObjectOne = s3Client.getObject(getObjectRequestOne);
    long partOneContentLength = s3ObjectOne.getObjectMetadata().getContentLength();
    assertEquals(allPartContentLength / 3, partOneContentLength);

    GetObjectRequest getObjectRequestTwo = new GetObjectRequest(bucketName, keyName);
    getObjectRequestTwo.setPartNumber(2);
    S3Object s3ObjectTwo = s3Client.getObject(getObjectRequestTwo);
    long partTwoContentLength = s3ObjectTwo.getObjectMetadata().getContentLength();
    assertEquals(allPartContentLength / 3, partTwoContentLength);

    GetObjectRequest getObjectRequestThree = new GetObjectRequest(bucketName, keyName);
    getObjectRequestThree.setPartNumber(1);
    S3Object s3ObjectThree = s3Client.getObject(getObjectRequestTwo);
    long partThreeContentLength = s3ObjectThree.getObjectMetadata().getContentLength();
    assertEquals(allPartContentLength / 3, partThreeContentLength);

    assertEquals(allPartContentLength, (partOneContentLength + partTwoContentLength + partThreeContentLength));
  }

  @Test
  public void testGetNotExistedPart(@TempDir Path tempDir) throws Exception {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();

    createFile(multipartUploadFile, (int) (15 * MB));

    multipartUpload(bucketName, keyName, multipartUploadFile, 5 * MB, null, null, null);

    GetObjectRequest getObjectRequestOne = new GetObjectRequest(bucketName, keyName);
    getObjectRequestOne.setPartNumber(4);
    S3Object s3ObjectOne = s3Client.getObject(getObjectRequestOne);
    long partOneContentLength = s3ObjectOne.getObjectMetadata().getContentLength();
    assertEquals(0, partOneContentLength);
  }

  @Test
  public void testListPartsNotFound() {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    ListPartsRequest listPartsRequest =
        new ListPartsRequest(bucketName, keyName, "nonexist");

    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.listParts(listPartsRequest));

    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(404, ase.getStatusCode());
    assertEquals("NoSuchUpload", ase.getErrorCode());
  }

  private boolean isBucketEmpty(Bucket bucket) {
    ObjectListing objectListing = s3Client.listObjects(bucket.getName());
    return objectListing.getObjectSummaries().isEmpty();
  }

  private String getBucketName() {
    return getBucketName(null);
  }

  private String getBucketName(String suffix) {
    return (getTestName() + "bucket" + suffix).toLowerCase(Locale.ROOT);
  }

  private String getKeyName() {
    return getKeyName(null);
  }

  private String getKeyName(String suffix) {
    return (getTestName() +  "key" + suffix).toLowerCase(Locale.ROOT);
  }

  private String multipartUpload(String bucketName, String key, File file, long partSize, String contentType,
                                 Map<String, String> userMetadata, List<Tag> tags) throws Exception {
    String uploadId = initiateMultipartUpload(bucketName, key, contentType, userMetadata, tags);

    List<PartETag> partETags = uploadParts(bucketName, key, uploadId, file, partSize);

    completeMultipartUpload(bucketName, key, uploadId, partETags);

    return uploadId;
  }

  private String initiateMultipartUpload(String bucketName, String key, String contentType,
                                         Map<String, String> metadata, List<Tag> tags) {
    InitiateMultipartUploadRequest initRequest;
    if (metadata == null || metadata.isEmpty()) {
      initRequest = new InitiateMultipartUploadRequest(bucketName, key);
    } else {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setUserMetadata(metadata);
      if (contentType != null) {
        objectMetadata.setContentType(contentType);
      }

      initRequest = new InitiateMultipartUploadRequest(bucketName, key, objectMetadata)
          .withTagging(new ObjectTagging(tags));
    }

    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
    assertEquals(bucketName, initResponse.getBucketName());
    assertEquals(key, initResponse.getKey());
    // TODO: Once bucket lifecycle configuration is supported, should check for "abortDate" and "abortRuleId"

    return initResponse.getUploadId();
  }

  // TODO: Also support async upload parts (similar to v2 asyncClient)
  private List<PartETag> uploadParts(String bucketName, String key, String uploadId, File file, long partSize)
      throws Exception {
    // Create a list of ETag objects. You retrieve ETags for each object part
    // uploaded,
    // then, after each individual part has been uploaded, pass the list of ETags to
    // the request to complete the upload.
    List<PartETag> partETags = new ArrayList<>();

    // Upload the file parts.
    long filePosition = 0;
    long fileLength = file.length();
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      for (int i = 1; filePosition < fileLength; i++) {
        // Because the last part could be less than 5 MB, adjust the part size as
        // needed.
        partSize = Math.min(partSize, (fileLength - filePosition));

        // Create the request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(bucketName)
            .withKey(key)
            .withUploadId(uploadId)
            .withPartNumber(i)
            .withFileOffset(filePosition)
            .withFile(file)
            .withPartSize(partSize);

        // Upload the part and add the response's ETag to our list.
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        PartETag partETag = uploadResult.getPartETag();
        assertEquals(i, partETag.getPartNumber());
        assertEquals(DatatypeConverter.printHexBinary(
            calculateDigest(fileInputStream, 0, (int) partSize)).toLowerCase(), partETag.getETag());
        partETags.add(partETag);

        filePosition += partSize;
      }
    }

    return partETags;
  }

  private void completeMultipartUpload(String bucketName, String key, String uploadId, List<PartETag> partETags) {
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, key,
        uploadId, partETags);
    CompleteMultipartUploadResult compResponse = s3Client.completeMultipartUpload(compRequest);
    assertEquals(bucketName, compResponse.getBucketName());
    assertEquals(key, compResponse.getKey());
  }

  private void abortMultipartUpload(String bucketName, String key, String uploadId) {
    AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucketName, key, uploadId);
    s3Client.abortMultipartUpload(abortRequest);
  }

  private static byte[] calculateDigest(InputStream inputStream, int skip, int length) throws Exception {
    int numRead;
    byte[] buffer = new byte[1024];

    MessageDigest complete = MessageDigest.getInstance("MD5");
    if (skip > -1 && length > -1) {
      inputStream = new InputSubstream(inputStream, skip, length);
    }

    do {
      numRead = inputStream.read(buffer);
      if (numRead > 0) {
        complete.update(buffer, 0, numRead);
      }
    } while (numRead != -1);

    return complete.digest();
  }

  private static void createFile(File newFile, int size) throws IOException {
    // write random data so that filesystems with compression enabled (e.g. ZFS)
    // can't compress the file
    Random random = new Random();
    byte[] data = new byte[size];
    random.nextBytes(data);

    RandomAccessFile file = new RandomAccessFile(newFile, "rws");

    file.write(data);

    file.getFD().sync();
    file.close();
  }
}
