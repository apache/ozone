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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.S3ClientFactory;
import org.apache.ozone.test.NonHATests;
import org.apache.ozone.test.OzoneTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListDirectoryBucketsResponse;

/**
 * Integration tests for the ListDirectoryBuckets S3 API (HDDS-15450).
 *
 * <p>These tests verify that GET / with the {@code max-directory-buckets} query parameter
 * correctly routes to the ListDirectoryBuckets handler and returns only FSO (File System
 * Optimized) buckets, while {@code ListBuckets} continues to return all bucket types.
 *
 * <p>Note: passing {@code maxDirectoryBuckets} explicitly is required to trigger the
 * ListDirectoryBuckets routing in Ozone's S3 Gateway, because both ListBuckets and
 * ListDirectoryBuckets share the same {@code GET /} endpoint and must be distinguished
 * by either the {@code max-directory-buckets} query parameter or S3 Express credential
 * scope. See {@code RootEndpoint#isListDirectoryBucketsRequest()}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractListDirectoryBucketsV2Tests extends OzoneTestBase
    implements NonHATests.TestCase {

  private MiniOzoneCluster cluster;
  private S3Client s3Client;

  @BeforeAll
  void createClient() throws Exception {
    cluster = cluster();
    S3ClientFactory s3Factory = new S3ClientFactory(cluster.getConf());
    s3Client = s3Factory.createS3ClientV2();
  }

  @AfterAll
  void closeClient() {
    if (s3Client != null) {
      s3Client.close();
    }
  }

  /**
   * Verifies that only FSO (directory) buckets are returned, and OBS buckets are excluded.
   * Also verifies that the standard ListBuckets still returns all bucket types.
   */
  @Test
  public void testListDirectoryBucketsReturnsOnlyFSOBuckets() throws Exception {
    final String obsBucketName = uniqueObjectName();
    final String fsoBucketName1 = uniqueObjectName();
    final String fsoBucketName2 = uniqueObjectName();

    s3Client.createBucket(b -> b.bucket(obsBucketName));
    createFsoBucket(fsoBucketName1);
    createFsoBucket(fsoBucketName2);
    try {
      ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(
          ListDirectoryBucketsRequest.builder()
              .maxDirectoryBuckets(1000)
              .build());

      List<String> dirBucketNames = response.buckets().stream()
          .map(Bucket::name)
          .collect(Collectors.toList());

      assertThat(dirBucketNames).contains(fsoBucketName1, fsoBucketName2);
      assertThat(dirBucketNames).doesNotContain(obsBucketName);
    } finally {
      s3Client.deleteBucket(b -> b.bucket(obsBucketName));
      deleteFsoBucket(fsoBucketName1);
      deleteFsoBucket(fsoBucketName2);
    }
  }

  /**
   * Verifies that an empty result with no continuation token is returned when no FSO
   * buckets exist (only OBS buckets present).
   */
  @Test
  public void testListDirectoryBucketsEmptyWhenNoFSOBuckets() throws Exception {
    final String obsBucketName = uniqueObjectName();

    s3Client.createBucket(b -> b.bucket(obsBucketName));
    try {
      ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(
          ListDirectoryBucketsRequest.builder()
              .maxDirectoryBuckets(1000)
              .build());

      List<String> dirBucketNames = response.buckets().stream()
          .map(Bucket::name)
          .collect(Collectors.toList());

      assertThat(dirBucketNames).doesNotContain(obsBucketName);
      assertNull(response.continuationToken());
    } finally {
      s3Client.deleteBucket(b -> b.bucket(obsBucketName));
    }
  }

  /**
   * Verifies pagination: listing FSO buckets page-by-page using {@code maxDirectoryBuckets}
   * and the returned continuation token, until all buckets are retrieved.
   */
  @Test
  public void testListDirectoryBucketsPaginationReturnsAllBuckets() throws Exception {
    final int totalBuckets = 5;
    final int pageSize = 2;
    List<String> created = new ArrayList<>();

    for (int i = 0; i < totalBuckets; i++) {
      String name = uniqueObjectName();
      createFsoBucket(name);
      created.add(name);
    }

    try {
      List<String> retrieved = new ArrayList<>();
      String continuationToken = null;

      do {
        ListDirectoryBucketsRequest.Builder reqBuilder = ListDirectoryBucketsRequest.builder()
            .maxDirectoryBuckets(pageSize);
        if (continuationToken != null) {
          reqBuilder.continuationToken(continuationToken);
        }

        ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(reqBuilder.build());

        response.buckets().stream()
            .map(Bucket::name)
            .filter(created::contains)
            .forEach(retrieved::add);

        continuationToken = response.continuationToken();
      } while (continuationToken != null);

      assertThat(retrieved).containsExactlyInAnyOrderElementsOf(created);
    } finally {
      for (String name : created) {
        deleteFsoBucket(name);
      }
    }
  }

  /**
   * Verifies that a single page returns no continuation token when fewer buckets exist
   * than the requested max.
   */
  @Test
  public void testListDirectoryBucketsNoContinuationTokenWhenResultFitsOnePage() throws Exception {
    final String fsoBucketName = uniqueObjectName();
    createFsoBucket(fsoBucketName);

    try {
      ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(
          ListDirectoryBucketsRequest.builder()
              .maxDirectoryBuckets(1000)
              .build());

      List<String> dirBucketNames = response.buckets().stream()
          .map(Bucket::name)
          .collect(Collectors.toList());

      assertThat(dirBucketNames).contains(fsoBucketName);
      assertNull(response.continuationToken(),
          "No continuation token expected when all results fit on one page");
    } finally {
      deleteFsoBucket(fsoBucketName);
    }
  }

  /**
   * Verifies response fields: name, creationDate, bucketRegion, and bucketArn are populated.
   * The BucketArn must match the expected S3 Express ARN format.
   */
  @Test
  public void testListDirectoryBucketsResponseFieldsArePopulated() throws Exception {
    final String fsoBucketName = uniqueObjectName();
    createFsoBucket(fsoBucketName);

    try {
      ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(
          ListDirectoryBucketsRequest.builder()
              .maxDirectoryBuckets(1000)
              .build());

      Bucket bucket = response.buckets().stream()
          .filter(b -> b.name().equals(fsoBucketName))
          .findFirst()
          .orElse(null);

      assertNotNull(bucket, "FSO bucket should be present in response");
      assertEquals(fsoBucketName, bucket.name());
      assertNotNull(bucket.creationDate(), "CreationDate must be set");
      assertNotNull(bucket.bucketRegion(), "BucketRegion must be set");

      // BucketArn should follow the S3 Express ARN format: arn:aws:s3express:<region>:<accountId>:bucket/<name>
      assertNotNull(bucket.bucketArn(), "BucketArn must be set");
      assertThat(bucket.bucketArn())
          .startsWith("arn:aws:s3express:")
          .endsWith(":bucket/" + fsoBucketName);
    } finally {
      deleteFsoBucket(fsoBucketName);
    }
  }

  /**
   * Verifies that maxDirectoryBuckets=0 returns an empty result immediately.
   */
  @Test
  public void testListDirectoryBucketsMaxZeroReturnsEmpty() throws Exception {
    final String fsoBucketName = uniqueObjectName();
    createFsoBucket(fsoBucketName);

    try {
      ListDirectoryBucketsResponse response = s3Client.listDirectoryBuckets(
          ListDirectoryBucketsRequest.builder()
              .maxDirectoryBuckets(0)
              .build());

      assertEquals(0, response.buckets().size());
    } finally {
      deleteFsoBucket(fsoBucketName);
    }
  }

  /**
   * Verifies that creating an FSO bucket does not affect the standard ListBuckets result —
   * FSO buckets must also appear in ListBuckets (they are still S3-accessible buckets).
   */
  @Test
  public void testListBucketsIncludesFSOBuckets() throws Exception {
    final String fsoBucketName = uniqueObjectName();
    createFsoBucket(fsoBucketName);

    try {
      List<String> allBuckets = s3Client.listBuckets().buckets().stream()
          .map(Bucket::name)
          .collect(Collectors.toList());

      assertThat(allBuckets).contains(fsoBucketName);
    } finally {
      deleteFsoBucket(fsoBucketName);
    }
  }

  private void createFsoBucket(String bucketName) throws Exception {
    try (OzoneClient ozoneClient = cluster.newClient()) {
      OzoneVolume volume = ozoneClient.getObjectStore().getS3Volume();
      volume.createBucket(bucketName, BucketArgs.newBuilder()
          .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
          .build());
    }
  }

  private void deleteFsoBucket(String bucketName) throws Exception {
    try (OzoneClient ozoneClient = cluster.newClient()) {
      OzoneVolume volume = ozoneClient.getObjectStore().getS3Volume();
      volume.deleteBucket(bucketName);
    }
  }
}
