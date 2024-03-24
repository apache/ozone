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

package org.apache.hadoop.ozone.s3g;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an abstract class to test the AWS Java S3 SDK operations.
 * This class should be extended for OM standalone and OM HA (Ratis) cluster setup.
 *
 * The test scenarios are adapted from https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/java/example_code/s3/.
 *
 * TODO: Currently we are using AWS SDK V1, might need to migrate to AWS SDK V2.
 */
@Timeout(300)
public abstract class TestBasicS3SDKAbstract {

  /**
   * Current unsupported S3 operations.
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
    TestBasicS3SDKAbstract.cluster = cluster;
  }

  public static MiniOzoneCluster getCluster() {
    return TestBasicS3SDKAbstract.cluster;
  }

  // Bucket Operations
  @Test
  public void testCreateBucket() {
    final AmazonS3 s3 = cluster.newS3Client();
    final String bucketName = UUID.randomUUID().toString();

    s3.createBucket(bucketName);
    assertTrue(s3.doesBucketExist(bucketName));
  }

  @Test
  public void testCreateBucket2() {

  }

  @Test
  public void testCreateBucketWithACL() {

  }
  @Test
  public void testListBuckets() {

  }

  @Test
  public void testDeleteBucket() {
    // TODO: Test the deleted bucket needs to be empty
  }


  // Object / Keys Operations

  @Test
  public void testUploadObject() {

  }

  @Test
  public void testGetObject() {

  }

  @Test
  public void testGetObjectWithoutETag() {
    // An object might not have ETag if it is uploaded using OFS protocol
  }

  @Test
  public void testGetObject2() {

  }

  @Test
  public void testListKeys() {

  }

  @Test
  public void testCopyObject() {
  }
  
  @Test
  public void testCopyObjectSingleOperation() {

  }

  @Test
  public void testHighLevelMultipartUpload() {

  }

  @Test
  public void testHighLevelTrackMultipartUpload() {

  }

  @Test
  public void testLowLevelMultipartUpload() {

  }

  @Test
  public void testLowLeveMultipartUpload() {

  }

  @Test
  public void generatePresignedPutURL() {

  }

  @Test
  public void generatePresignedURL() {

  }

  @Test
  public void generatedPresignedURLAndUploadObject() {

  }

  @Test
  public void transferManagerCopy() {

  }

  @Test
  public void transferManagerProgress() {

  }

  @Test
  public void transferManagerUpload() {

  }

}
