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

package org.apache.hadoop.ozone.s3.awssdk.v1;

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.calculateDigest;
import static org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils.createFile;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetBucketLifecycleConfigurationRequest;
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
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetObjectAclRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.hadoop.ozone.s3.awssdk.S3SDKTestUtils;
import org.apache.hadoop.ozone.s3.endpoint.S3Owner;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.NonHATests;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from
 * - https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/java/example_code/s3/
 * - https://github.com/ceph/s3-tests
 *
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractS3SDKV1Tests extends OzoneTestBase implements NonHATests.TestCase {

  // server-side limitation
  private static final int MAX_UPLOADS_LIMIT = 1000;

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

  private MiniOzoneCluster cluster;
  private AmazonS3 s3Client;

  @BeforeAll
  void createClient() {
    cluster = cluster();
    s3Client = new S3ClientFactory(cluster.getConf())
        .createS3Client();
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
  public void testListBuckets() throws IOException {
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

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String expectOwner = ugi.getShortUserName();

    Owner s3AccountOwner = s3Client.getS3AccountOwner();

    assertThat(s3AccountOwner.getDisplayName()).isEqualTo(expectOwner);
    assertThat(s3AccountOwner.getId()).isEqualTo(S3Owner.DEFAULT_S3OWNER_ID);
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
      s3Client.putObject(bucketName, "key-" + i, RandomStringUtils.secure().nextAlphanumeric(1024));
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
  public void testPutDoubleSlashPrefixObject() throws IOException {
    final String bucketName = getBucketName();
    final String keyName = "//dir1";
    final String content = "bar";
    // Create a FSO bucket for test
    try (OzoneClient ozoneClient = cluster.newClient()) {
      ObjectStore store = ozoneClient.getObjectStore();
      OzoneVolume volume = store.getS3Volume();
      OmBucketInfo.Builder bucketInfo = new OmBucketInfo.Builder()
          .setVolumeName(volume.getName())
          .setBucketName(bucketName)
          .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      OzoneManagerProtocol ozoneManagerProtocol = store.getClientProxy().getOzoneManagerClient();
      ozoneManagerProtocol.createBucket(bucketInfo.build());
    }

    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    PutObjectResult putObjectResult = s3Client.putObject(bucketName, keyName, is, new ObjectMetadata());
    assertEquals("37b51d194a7513e45b56f6524f2d51f2", putObjectResult.getETag());

    S3Object object = s3Client.getObject(bucketName, keyName);
    assertEquals(content.length(), object.getObjectMetadata().getContentLength());
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

    try (OzoneClient ozoneClient = cluster.newClient()) {
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
  public void testListObjectsMany() throws Exception {
    testListObjectsMany(false);
  }

  @Test
  public void testListObjectsManyV2() throws Exception {
    testListObjectsMany(true);
  }

  private void testListObjectsMany(boolean isListV2) throws Exception {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);
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
      PutObjectResult putObjectResult = s3Client.putObject(bucketName, keyName,
          RandomStringUtils.secure().nextAlphanumeric(5));
      keyToEtag.put(keyName, putObjectResult.getETag());
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

    List<S3ObjectSummary> objectSummaries;
    String continuationToken;
    if (isListV2) {
      ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
          .withBucketName(bucketName)
          .withMaxKeys(2);
      ListObjectsV2Result listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
      objectSummaries = listObjectsResponse.getObjectSummaries();
      assertEquals(bucketName, listObjectsResponse.getBucketName());
      assertTrue(listObjectsResponse.isTruncated());
      continuationToken = listObjectsResponse.getNextContinuationToken();
    } else {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucketName)
          .withMaxKeys(2);
      ObjectListing listObjectsResponse = s3Client.listObjects(listObjectsRequest);
      objectSummaries = listObjectsResponse.getObjectSummaries();
      assertEquals(bucketName, listObjectsResponse.getBucketName());
      assertTrue(listObjectsResponse.isTruncated());
      continuationToken = listObjectsResponse.getNextMarker();
    }
    assertThat(objectSummaries).hasSize(2);
    assertEquals(objectSummaries.stream()
            .map(S3ObjectSummary::getKey).collect(Collectors.toList()),
        keyNames.subList(0, 2));
    for (S3ObjectSummary objectSummary : objectSummaries) {
      assertEquals(keyToEtag.get(objectSummary.getKey()), objectSummary.getETag());
    }

    // Include both keys with and without ETag
    if (isListV2) {
      ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
          .withBucketName(bucketName)
          .withMaxKeys(5)
          .withContinuationToken(continuationToken);
      ListObjectsV2Result listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
      objectSummaries = listObjectsResponse.getObjectSummaries();
      assertEquals(bucketName, listObjectsResponse.getBucketName());
      assertFalse(listObjectsResponse.isTruncated());
    } else {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucketName)
          .withMaxKeys(5)
          .withMarker(continuationToken);
      ObjectListing listObjectsResponse = s3Client.listObjects(listObjectsRequest);
      objectSummaries = listObjectsResponse.getObjectSummaries();
      assertEquals(bucketName, listObjectsResponse.getBucketName());
      assertFalse(listObjectsResponse.isTruncated());
    }

    assertThat(objectSummaries).hasSize(3);
    assertEquals(keyNames.get(2), objectSummaries.get(0).getKey());
    assertEquals(keyNamesWithoutETag.get(0), objectSummaries.get(1).getKey());
    assertEquals(keyNamesWithoutETag.get(1), objectSummaries.get(2).getKey());
    for (S3ObjectSummary objectSummary : objectSummaries) {
      assertEquals(keyToEtag.get(objectSummary.getKey()), objectSummary.getETag());
    }
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

    String uploadId3 = initiateMultipartUpload(bucketName, multipartKey2, null, null, null);
    uploadIds.add(uploadId3);

    ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);
    listMultipartUploadsRequest.setMaxUploads(5000);

    MultipartUploadListing result = s3Client.listMultipartUploads(listMultipartUploadsRequest);

    List<String> listUploadIds = result.getMultipartUploads().stream()
        .map(MultipartUpload::getUploadId)
        .collect(Collectors.toList());

    assertEquals(uploadIds, listUploadIds);
  }

  @ParameterizedTest
  @ValueSource(ints = {10, 5000})
  public void testListMultipartUploadsPagination(int requestedMaxUploads) {
    final String bucketName = getBucketName() + "-" + requestedMaxUploads;
    final String multipartKeyPrefix = getKeyName("multipart");

    s3Client.createBucket(bucketName);

    // Create multipart uploads to test pagination
    List<String> allKeys = new ArrayList<>();
    Map<String, String> keyToUploadId = new HashMap<>();

    final int effectiveMaxUploads = Math.min(requestedMaxUploads, MAX_UPLOADS_LIMIT);
    final int uploadsCreated = 2 * effectiveMaxUploads + 5;
    final int expectedPages = uploadsCreated / effectiveMaxUploads + 1;

    for (int i = 0; i < uploadsCreated; i++) {
      String key = String.format("%s-%04d", multipartKeyPrefix, i);
      allKeys.add(key);
      String uploadId = initiateMultipartUpload(bucketName, key, null, null, null);
      keyToUploadId.put(key, uploadId);
    }
    Collections.sort(allKeys);

    // Test pagination
    Set<String> retrievedKeys = new HashSet<>();
    String keyMarker = null;
    String uploadIdMarker = null;
    boolean truncated = true;
    int pageCount = 0;

    do {
      ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucketName)
          .withMaxUploads(requestedMaxUploads)
          .withKeyMarker(keyMarker)
          .withUploadIdMarker(uploadIdMarker);

      MultipartUploadListing result = s3Client.listMultipartUploads(request);
      pageCount++;

      // Verify page size
      if (pageCount < expectedPages) {
        assertEquals(effectiveMaxUploads, result.getMultipartUploads().size());
        assertTrue(result.isTruncated());
      } else {
        assertEquals(uploadsCreated % effectiveMaxUploads, result.getMultipartUploads().size());
        assertFalse(result.isTruncated());
      }

      // Collect keys and verify uploadIds
      for (MultipartUpload upload : result.getMultipartUploads()) {
        String key = upload.getKey();
        retrievedKeys.add(key);
        assertEquals(keyToUploadId.get(key), upload.getUploadId());
      }

      // Verify response
      assertNull(result.getPrefix());
      assertEquals(result.getUploadIdMarker(), uploadIdMarker);
      assertEquals(result.getKeyMarker(), keyMarker);
      assertEquals(effectiveMaxUploads, result.getMaxUploads());

      // Verify next markers content
      if (result.isTruncated()) {
        MultipartUpload lastUploadOnPage = result.getMultipartUploads()
            .get(result.getMultipartUploads().size() - 1);
        assertEquals(lastUploadOnPage.getKey(), result.getNextKeyMarker());
        assertEquals(lastUploadOnPage.getUploadId(), result.getNextUploadIdMarker());
      } else {
        assertNull(result.getNextKeyMarker());
        assertNull(result.getNextUploadIdMarker());
      }

      // Update markers for next page
      keyMarker = result.getNextKeyMarker();
      uploadIdMarker = result.getNextUploadIdMarker();

      truncated = result.isTruncated();

    } while (truncated);

    // Verify pagination results
    assertEquals(expectedPages, pageCount);
    assertEquals(uploadsCreated, retrievedKeys.size(), "Should retrieve all uploads");
    assertEquals(
        allKeys,
        retrievedKeys.stream().sorted().collect(Collectors.toList()),
        "Retrieved keys should match expected keys in order");

    // Test with prefix
    String prefix = multipartKeyPrefix + "-001";
    ListMultipartUploadsRequest prefixRequest = new ListMultipartUploadsRequest(bucketName)
        .withPrefix(prefix);

    MultipartUploadListing prefixResult = s3Client.listMultipartUploads(prefixRequest);

    assertEquals(prefix, prefixResult.getPrefix());
    assertEquals(
        IntStream.rangeClosed(0, 9)
            .mapToObj(i -> prefix + i)
            .collect(Collectors.toList()),
        prefixResult.getMultipartUploads().stream()
            .map(MultipartUpload::getKey)
            .collect(Collectors.toList()));
  }

  @Test
  public void testListMultipartUploadsPaginationCornerCases() {
    final String bucketName = getBucketName();
    final String keyA = getKeyName("samekey");
    final String keyB = getKeyName("after");

    s3Client.createBucket(bucketName);

    // Create multiple MPUs for the same key to verify upload-id-marker semantics
    List<String> keyAUploadIds = new ArrayList<>();
    keyAUploadIds.add(initiateMultipartUpload(bucketName, keyA, null, null, null));
    keyAUploadIds.add(initiateMultipartUpload(bucketName, keyA, null, null, null));
    keyAUploadIds.add(initiateMultipartUpload(bucketName, keyA, null, null, null));
    // Also create another key to ensure listing proceeds past keyA
    initiateMultipartUpload(bucketName, keyB, null, null, null);

    // Sort upload IDs lexicographically to match listing order for the same key
    Collections.sort(keyAUploadIds);

    // Case 1: key-marker=keyA and upload-id-marker set to the lowest uploadId
    // Per spec, same-key uploads MAY be included if uploadId > marker
    ListMultipartUploadsRequest request1 = new ListMultipartUploadsRequest(bucketName)
        .withKeyMarker(keyA)
        .withUploadIdMarker(keyAUploadIds.get(0))
        .withMaxUploads(100);
    MultipartUploadListing result1 = s3Client.listMultipartUploads(request1);

    List<MultipartUpload> uploads1 = result1.getMultipartUploads();
    // Collect same-key uploads and verify none are <= marker
    List<String> sameKeyIds1 = uploads1.stream()
        .filter(u -> keyA.equals(u.getKey()))
        .map(MultipartUpload::getUploadId)
        .collect(Collectors.toList());
    assertThat(sameKeyIds1).allSatisfy(id -> assertTrue(id.compareTo(keyAUploadIds.get(0)) > 0));

    // Case 2: key-marker=keyA and upload-id-marker set to the highest uploadId
    // Expect no same-key (keyA) uploads to be returned
    ListMultipartUploadsRequest request2 = new ListMultipartUploadsRequest(bucketName)
        .withKeyMarker(keyA)
        .withUploadIdMarker(keyAUploadIds.get(2))
        .withMaxUploads(100);
    MultipartUploadListing result2 = s3Client.listMultipartUploads(request2);
    assertTrue(result2.getMultipartUploads().stream().noneMatch(u -> keyA.equals(u.getKey())));
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

  @Test
  public void testQuotaExceeded() throws IOException {
    final String bucketName = getBucketName();
    final String keyName = getKeyName();

    s3Client.createBucket(bucketName);

    cluster.newClient().getObjectStore()
        .getVolume("s3v")
        .getBucket(bucketName)
        .setQuota(OzoneQuota.parseQuota("1", "10"));

    // Upload some objects to the bucket
    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.putObject(bucketName, keyName,
            RandomStringUtils.secure().nextAlphanumeric(1024)));

    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(403, ase.getStatusCode());
    assertEquals("QuotaExceeded", ase.getErrorCode());
  }

  @Test
  public void testS3LifecycleConfigurationCreateSuccessfully() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);
    BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration();
    List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();
    BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
        .withId("expire-logs-after-365-days")
        .withPrefix("logs/")
        .withStatus(BucketLifecycleConfiguration.ENABLED)
        .withExpirationInDays(365);
    rules.add(rule1);

    configuration.setRules(rules);

    // Set lifecycle configuration
    SetBucketLifecycleConfigurationRequest request =
        new SetBucketLifecycleConfigurationRequest(bucketName, configuration);
    s3Client.setBucketLifecycleConfiguration(request);

    // Verify the configuration was set
    BucketLifecycleConfiguration retrievedConfig =
        s3Client.getBucketLifecycleConfiguration(bucketName);
    assertEquals(1, retrievedConfig.getRules().size());

    // Verify rule 1
    BucketLifecycleConfiguration.Rule retrievedRule1 = retrievedConfig.getRules().get(0);
    assertEquals("expire-logs-after-365-days", retrievedRule1.getId());
    assertEquals("logs/", retrievedRule1.getPrefix());
    assertEquals(BucketLifecycleConfiguration.ENABLED, retrievedRule1.getStatus());
    assertEquals(365, retrievedRule1.getExpirationInDays());
  }

  @Test
  public void testS3LifecycleConfigurationCreationFailed() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);

    // Test 1: Invalid configuration
    BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration();
    List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();

    BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
        .withId("invalid")
        .withStatus(BucketLifecycleConfiguration.ENABLED);
    rules.add(rule);
    configuration.setRules(rules);
    SetBucketLifecycleConfigurationRequest request =
        new SetBucketLifecycleConfigurationRequest(bucketName, configuration);

    AmazonServiceException ase = assertThrows(AmazonServiceException.class,
        () -> s3Client.setBucketLifecycleConfiguration(request));
    assertEquals(ErrorType.Client, ase.getErrorType());
    assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, ase.getStatusCode());

    // Test 2: Non-existent bucket
    final String nonExistentBucket = getBucketName("nonexistent");
    BucketLifecycleConfiguration validConfig = new BucketLifecycleConfiguration();
    List<BucketLifecycleConfiguration.Rule> validRules = new ArrayList<>();
    BucketLifecycleConfiguration.Rule validRule = new BucketLifecycleConfiguration.Rule()
        .withId("test-rule")
        .withPrefix("test/")
        .withStatus(BucketLifecycleConfiguration.ENABLED)
        .withExpirationInDays(30);
    validRules.add(validRule);
    validConfig.setRules(validRules);

    SetBucketLifecycleConfigurationRequest nonExistentRequest =
        new SetBucketLifecycleConfigurationRequest(nonExistentBucket, validConfig);

    AmazonServiceException ase2 = assertThrows(AmazonServiceException.class,
        () -> s3Client.setBucketLifecycleConfiguration(nonExistentRequest));
    assertEquals(ErrorType.Client, ase2.getErrorType());
    assertEquals(HttpURLConnection.HTTP_NOT_FOUND, ase2.getStatusCode());
    assertEquals("NoSuchBucket", ase2.getErrorCode());
  }

  @Test
  public void testS3LifecycleConfigurationDelete() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);

    // Test delete lifecycle for a bucket, while doesn't have lifecycle
    assertNull(s3Client.getBucketLifecycleConfiguration(bucketName));
    assertThrows(AmazonServiceException.class,
        () -> s3Client.deleteBucketLifecycleConfiguration(bucketName));

    // First create a lifecycle configuration
    BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration();
    List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();
    BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
        .withId("test-rule")
        .withPrefix("test/")
        .withStatus(BucketLifecycleConfiguration.ENABLED)
        .withExpirationInDays(30);
    rules.add(rule);
    configuration.setRules(rules);

    s3Client.setBucketLifecycleConfiguration(bucketName, configuration);
    // Verify it exists
    BucketLifecycleConfiguration retrievedConfig =
        s3Client.getBucketLifecycleConfiguration(bucketName);
    assertEquals(1, retrievedConfig.getRules().size());
    // Delete the lifecycle configuration
    s3Client.deleteBucketLifecycleConfiguration(bucketName);
    assertNull(s3Client.getBucketLifecycleConfiguration(bucketName));
    // Test delete on non-existent bucket
    final String nonExistentBucket = getBucketName("nonexistent");
    assertThrows(AmazonServiceException.class,
        () -> s3Client.deleteBucketLifecycleConfiguration(nonExistentBucket));
  }

  @Test
  public void testS3LifecycleConfigurationGet() {
    final String bucketName = getBucketName();
    s3Client.createBucket(bucketName);

    // Test get on bucket without lifecycle configuration
    assertNull(s3Client.getBucketLifecycleConfiguration(bucketName));

    // Create a comprehensive lifecycle configuration
    BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration();
    List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<>();

    // Rule with expiration and prefix
    BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
        .withId("expire-old-files")
        .withPrefix("old/")
        .withStatus(BucketLifecycleConfiguration.ENABLED)
        .withExpirationInDays(365);

    rules.add(rule1);
    configuration.setRules(rules);

    // Set the configuration
    s3Client.setBucketLifecycleConfiguration(bucketName, configuration);

    // Get and verify the configuration
    BucketLifecycleConfiguration retrievedConfig =
        s3Client.getBucketLifecycleConfiguration(bucketName);

    assertEquals(1, retrievedConfig.getRules().size());

    // Verify first rule
    BucketLifecycleConfiguration.Rule retrievedRule1 = retrievedConfig.getRules().get(0);
    assertEquals("expire-old-files", retrievedRule1.getId());
    assertEquals("old/", retrievedRule1.getPrefix());
    assertEquals(BucketLifecycleConfiguration.ENABLED, retrievedRule1.getStatus());
    assertEquals(365, retrievedRule1.getExpirationInDays());

    // Test getting configuration using GetBucketLifecycleConfigurationRequest
    GetBucketLifecycleConfigurationRequest getRequest =
        new GetBucketLifecycleConfigurationRequest(bucketName);
    BucketLifecycleConfiguration configFromRequest =
        s3Client.getBucketLifecycleConfiguration(getRequest);
    assertEquals(retrievedConfig.getRules().size(), configFromRequest.getRules().size());
  }

  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class PresignedUrlTests {
    private static final String BUCKET_NAME = "v1-presigned-url-bucket";
    private static final String CONTENT = "bar";
    // Set the presigned URL to expire after one hour.
    private final Date expiration = Date.from(Instant.now().plusMillis(1000 * 60 * 60));

    @BeforeAll
    public void setup() {
      s3Client.createBucket(BUCKET_NAME);
    }

    @Test
    public void testPresignedUrlGet() throws IOException {
      final String keyName = getKeyName();

      InputStream is = new ByteArrayInputStream(CONTENT.getBytes(StandardCharsets.UTF_8));

      s3Client.putObject(BUCKET_NAME, keyName, is, new ObjectMetadata());

      // Generate the presigned URL
      GeneratePresignedUrlRequest generatePresignedUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.GET).withExpiration(expiration);
      generatePresignedUrlRequest.addRequestParameter("x-custom-parameter", "custom-value");
      URL presignedUrl = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

      // Download the object using HttpUrlConnection (since v1.1)
      // Capture the response body to a byte array.
      HttpURLConnection connection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "GET", null, null);
      // Download the result of executing the request.
      try (InputStream s3is = connection.getInputStream();
           ByteArrayOutputStream bos = new ByteArrayOutputStream(CONTENT.getBytes(StandardCharsets.UTF_8).length)) {
        IOUtils.copy(s3is, bos);
        assertEquals(CONTENT, bos.toString("UTF-8"));
      }
    }

    @Test
    public void testPresignedUrlHead() throws IOException {
      final String keyName = getKeyName();

      InputStream is = new ByteArrayInputStream(CONTENT.getBytes(StandardCharsets.UTF_8));
      s3Client.putObject(BUCKET_NAME, keyName, is, new ObjectMetadata());

      // Test HeadObject presigned URL
      GeneratePresignedUrlRequest generatePresignedUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.HEAD).withExpiration(expiration);
      URL presignedUrl = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

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

      // Test HeadBucket presigned URL
      GeneratePresignedUrlRequest generateBucketPresignedUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, null).withMethod(HttpMethod.HEAD).withExpiration(expiration);
      URL presignedBucketUrl = s3Client.generatePresignedUrl(generateBucketPresignedUrlRequest);

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
    }

    @Test
    public void testPresignedUrlPutObject() throws Exception {
      final String keyName = getKeyName();

      // Test PutObjectRequest presigned URL
      GeneratePresignedUrlRequest generatePresignedUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.PUT).withExpiration(expiration);
      URL presignedUrl = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

      HttpURLConnection connection = null;
      try {
        connection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "PUT",
            null, CONTENT.getBytes(StandardCharsets.UTF_8));
        int responseCode = connection.getResponseCode();
        assertEquals(200, responseCode, "PutObject presigned URL should return 200 OK");
        String actualContent;
        S3Object s3Object = s3Client.getObject(BUCKET_NAME, keyName);
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
          actualContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        assertEquals(CONTENT, actualContent, "Downloaded content should match uploaded content");
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    @Test
    public void testPresignedUrlMultipartUpload(@TempDir Path tempDir) throws Exception {
      final String keyName = getKeyName();
      final Map<String, String> userMetadata = new HashMap<>();
      userMetadata.put("key1", "value1");
      userMetadata.put("key2", "value2");
      final Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "value1");
      tags.put("tag2", "value2");

      File multipartUploadFile = Files.createFile(tempDir.resolve("multipartupload.txt")).toFile();
      createFile(multipartUploadFile, (int) (10 * MB));

      // create MPU using presigned URL
      GeneratePresignedUrlRequest initMPUPresignedUrlRequest =
          createInitMPUPresignedUrlRequest(keyName, userMetadata, tags);


      String uploadId = initMultipartUpload(initMPUPresignedUrlRequest);

      // upload parts using presigned URL
      List<PartETag> completedParts = uploadParts(multipartUploadFile, keyName, uploadId);

      // Complete multipart upload using presigned URL
      completeMPU(keyName, uploadId, completedParts);

      // Verify upload result
      ObjectMetadata objectMeta = s3Client.getObjectMetadata(BUCKET_NAME, keyName);
      assertEquals(userMetadata, objectMeta.getUserMetadata());

      // Verify content
      S3Object s3Object = s3Client.getObject(BUCKET_NAME, keyName);
      assertEquals(tags.size(), s3Object.getTaggingCount());
      String actualContent;
      try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        actualContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      }
      String expectedContent = new String(Files.readAllBytes(multipartUploadFile.toPath()), StandardCharsets.UTF_8);
      assertEquals(expectedContent, actualContent, "Downloaded content should match uploaded content");
    }

    private void completeMPU(String keyName, String uploadId, List<PartETag> completedParts) throws IOException {
      GeneratePresignedUrlRequest request =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.POST).withExpiration(expiration);
      request.addRequestParameter("uploadId", uploadId);

      URL presignedUrl = s3Client.generatePresignedUrl(request);

      // Generate completion XML payload
      StringBuilder completionXml = new StringBuilder();
      completionXml.append("<CompleteMultipartUpload>\n");
      for (PartETag part : completedParts) {
        completionXml.append("  <Part>\n");
        completionXml.append("    <PartNumber>").append(part.getPartNumber()).append("</PartNumber>\n");
        completionXml.append("    <ETag>").append(stripQuotes(part.getETag())).append("</ETag>\n");
        completionXml.append("  </Part>\n");
      }
      completionXml.append("</CompleteMultipartUpload>");

      byte[] completionPayloadBytes = completionXml.toString().getBytes(StandardCharsets.UTF_8);

      HttpURLConnection httpConnection = null;
      try {
        httpConnection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "POST", null, completionPayloadBytes);
        int responseCode = httpConnection.getResponseCode();
        assertEquals(200, responseCode, "Complete multipart upload should return 200 OK");
      } finally {
        if (httpConnection != null) {
          httpConnection.disconnect();
        }
      }
    }

    private List<PartETag> uploadParts(File multipartUploadFile, String keyName, String uploadId) throws IOException {
      List<PartETag> completedParts = new ArrayList<>();
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) (5 * MB));
      long filePosition = 0;
      long fileLength = multipartUploadFile.length();
      int partNumber = 1;

      try (RandomAccessFile file = new RandomAccessFile(multipartUploadFile, "r")) {
        while (filePosition < fileLength) {
          file.seek(filePosition);
          long bytesRead = file.getChannel().read(byteBuffer);
          byteBuffer.flip();

          // generate presigned URL for each part
          GeneratePresignedUrlRequest request =
              new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.PUT)
                  .withExpiration(expiration);
          request.addRequestParameter("partNumber", String.valueOf(partNumber));
          request.addRequestParameter("uploadId", uploadId);

          URL presignedUrl = s3Client.generatePresignedUrl(request);

          // upload each part using presigned URL
          HttpURLConnection connection = null;
          try {
            Map<String, List<String>> headers = new HashMap<>();
            List<String> header = Collections.singletonList(String.valueOf(byteBuffer.remaining()));
            headers.put("Content-Length", header);
            byte[] body = new byte[byteBuffer.remaining()];
            byteBuffer.get(body);

            connection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "PUT", headers, body);
            int responseCode = connection.getResponseCode();
            assertEquals(200, responseCode, String.format("Upload part %d should return 200 OK", partNumber));

            String etag = connection.getHeaderField("ETag");
            PartETag partETag = new PartETag(partNumber, etag);
            completedParts.add(partETag);
          } finally {
            if (connection != null) {
              connection.disconnect();
            }
          }

          byteBuffer.clear();
          filePosition += bytesRead;
          partNumber++;
        }
      }
      return completedParts;
    }

    private String initMultipartUpload(GeneratePresignedUrlRequest request) throws IOException {
      URL presignedUrl = s3Client.generatePresignedUrl(request);
      String uploadId;
      HttpURLConnection httpConnection = null;
      try {
        Map<String, String> customRequestHeaders = request.getCustomRequestHeaders();
        Map<String, List<String>> headers = new HashMap<>();
        if (customRequestHeaders != null) {
          for (Map.Entry<String, String> entry : customRequestHeaders.entrySet()) {
            headers.put(entry.getKey(), Collections.singletonList(entry.getValue()));
          }
        }
        httpConnection = S3SDKTestUtils.openHttpURLConnection(presignedUrl, "POST", headers, null);
        int initMPUConnectionResponseCode = httpConnection.getResponseCode();
        assertEquals(200, initMPUConnectionResponseCode);

        try (InputStream is = httpConnection.getInputStream()) {
          String responseXml = IOUtils.toString(is, StandardCharsets.UTF_8);
          uploadId = S3SDKTestUtils.extractUploadId(responseXml);
        }
      } finally {
        if (httpConnection != null) {
          httpConnection.disconnect();
        }
      }
      return uploadId;
    }

    private GeneratePresignedUrlRequest createInitMPUPresignedUrlRequest(String keyName,
                                                                         Map<String, String> userMetadata,
                                                                         Map<String, String> tags) throws Exception {
      GeneratePresignedUrlRequest initMPUPresignUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.POST).withExpiration(expiration);

      userMetadata.forEach((k, v) -> {
        initMPUPresignUrlRequest.putCustomRequestHeader(CUSTOM_METADATA_HEADER_PREFIX + k, v);
      });

      StringBuilder tagValueBuilder = new StringBuilder();
      for (Map.Entry<String, String> entry : tags.entrySet()) {
        if (tagValueBuilder.length() > 0) {
          tagValueBuilder.append('&');
        }
        tagValueBuilder.append(entry.getKey()).append('=').append(URLEncoder.encode(entry.getValue(), "UTF-8"));
      }
      initMPUPresignUrlRequest.putCustomRequestHeader(S3Consts.TAG_HEADER, tagValueBuilder.toString());
      return initMPUPresignUrlRequest;
    }

    @Test
    public void testPresignedUrlDelete() throws IOException {
      final String keyName = getKeyName();

      try (InputStream is = new ByteArrayInputStream(CONTENT.getBytes(StandardCharsets.UTF_8))) {
        s3Client.putObject(BUCKET_NAME, keyName, is, new ObjectMetadata());
      }

      // Generate the presigned URL for DELETE
      GeneratePresignedUrlRequest generatePresignedUrlRequest =
          new GeneratePresignedUrlRequest(BUCKET_NAME, keyName).withMethod(HttpMethod.DELETE)
              .withExpiration(expiration);
      URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);

      // Execute the DELETE request using HttpUrlConnection
      HttpURLConnection connection = null;
      try {
        connection = S3SDKTestUtils.openHttpURLConnection(url, "DELETE", null, null);
        int responseCode = connection.getResponseCode();
        // Verify the response code is 204 (No Content)
        assertEquals(HttpURLConnection.HTTP_NO_CONTENT, responseCode);
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }

      // Verify the object is deleted
      assertFalse(s3Client.doesObjectExist(BUCKET_NAME, keyName));
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
    final byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    // Create the bucket and upload an object using S3 SDK.
    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    s3Client.createBucket(bucketName);
    s3Client.putObject(bucketName, keyName,
        is, new ObjectMetadata());

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

    S3Object s3Object = s3Client.getObject(bucketName, snapshotKey);
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

  private boolean isBucketEmpty(Bucket bucket) {
    ObjectListing objectListing = s3Client.listObjects(bucket.getName());
    return objectListing.getObjectSummaries().isEmpty();
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
    try (InputStream fileInputStream = Files.newInputStream(file.toPath())) {
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
}
