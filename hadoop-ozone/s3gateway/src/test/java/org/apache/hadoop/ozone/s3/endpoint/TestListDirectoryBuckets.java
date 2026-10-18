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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.commontypes.DirectoryBucketMetadata;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ListDirectoryBuckets (FSO bucket listing).
 */
public class TestListDirectoryBuckets {

  private OzoneClient clientStub;
  private RootEndpoint rootEndpoint;

  private static final String DEFAULT_VOLUME = OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;

  @BeforeEach
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    rootEndpoint = EndpointBuilder.newRootEndpointBuilder()
        .setClient(clientStub)
        .setSignatureInfo(new SignatureInfo.Builder(SignatureInfo.Version.V4)
            .setCredentialScope("20260101/us-west-2/s3express/aws4_request")
            .build())
        .build();

    clientStub.getObjectStore().createVolume(DEFAULT_VOLUME);
  }

  @Test
  public void testListDirectoryBucketsEmpty() throws Exception {
    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, 1000);

    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(0, response.getBucketsNum());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListDirectoryBucketsFiltersNonFsoBuckets() throws Exception {
    clientStub.getObjectStore().createS3Bucket("obs-bucket");
    createFsoBucket("fso-bucket-1");
    createFsoBucket("fso-bucket-2");

    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, 1000);
    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(2, response.getBucketsNum());
    assertNull(response.getContinuationToken());

    DirectoryBucketMetadata first = response.getBuckets().get(0);
    assertEquals("fso-bucket-1", first.getName());
    assertEquals("us-west-2", first.getBucketRegion());
    assertEquals(
        RootEndpoint.buildDirectoryBucketArn("us-west-2", S3Owner.DEFAULT_S3OWNER_ID,
            "fso-bucket-1"),
        first.getBucketArn());
    assertNotNull(first.getCreationDate());
  }

  @Test
  public void testListDirectoryBucketsPagination() throws Exception {
    for (int i = 0; i < 5; i++) {
      createFsoBucket("fso-bucket-" + i);
    }

    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, 2);
    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(2, response.getBucketsNum());
    assertEquals("fso-bucket-0", response.getBuckets().get(0).getName());
    assertEquals("fso-bucket-1", response.getBuckets().get(1).getName());
    assertNotNull(response.getContinuationToken());

    rootEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
        response.getContinuationToken());
    response = (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(2, response.getBucketsNum());
    assertEquals("fso-bucket-2", response.getBuckets().get(0).getName());
    assertEquals("fso-bucket-3", response.getBuckets().get(1).getName());
    assertNotNull(response.getContinuationToken());

    rootEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
        response.getContinuationToken());
    response = (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(1, response.getBucketsNum());
    assertEquals("fso-bucket-4", response.getBuckets().get(0).getName());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListDirectoryBucketsZeroMaxBuckets() throws Exception {
    createFsoBucket("fso-bucket");

    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, 0);
    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(0, response.getBucketsNum());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListDirectoryBucketsInvalidMaxBuckets() {
    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, -1);
    assertThrows(OS3Exception.class, () -> rootEndpoint.get());
  }

  @Test
  public void testListDirectoryBucketsWithS3ExpressSigningOnly() throws Exception {
    clientStub.getObjectStore().createS3Bucket("obs-bucket");
    createFsoBucket("fso-bucket");

    rootEndpoint = EndpointBuilder.newRootEndpointBuilder()
        .setClient(clientStub)
        .setSignatureInfo(new SignatureInfo.Builder(SignatureInfo.Version.V4)
            .setCredentialScope("20260101/aws-global/s3express/aws4_request")
            .build())
        .build();

    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(1, response.getBucketsNum());
    assertEquals("fso-bucket", response.getBuckets().get(0).getName());
    assertEquals("aws-global", response.getBuckets().get(0).getBucketRegion());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListDirectoryBucketsCapsAtLimit() throws Exception {
    for (int i = 0; i < 3; i++) {
      createFsoBucket("fso-bucket-" + i);
    }

    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_DIRECTORY_BUCKETS, 2000);
    ListDirectoryBucketsResponse response =
        (ListDirectoryBucketsResponse) rootEndpoint.get().getEntity();

    assertEquals(3, response.getBucketsNum());
    assertNull(response.getContinuationToken());
  }

  private void createFsoBucket(String bucketName) throws Exception {
    OzoneVolume volume = clientStub.getObjectStore().getVolume(DEFAULT_VOLUME);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build());
  }
}
