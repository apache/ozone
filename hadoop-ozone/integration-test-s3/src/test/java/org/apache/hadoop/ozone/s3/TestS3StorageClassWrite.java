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

package org.apache.hadoop.ozone.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/**
 * S3 writes that request a storage class record the class's storage policy on the key. Every datanode has DISK, SSD
 * and ARCHIVE volumes, so a COLD key can be placed.
 */
class TestS3StorageClassWrite extends ClusterForTests<MiniOzoneCluster> {

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);
  private static final String CONTENT = "storage-class";

  private final S3GatewayService s3g = new S3GatewayService();

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder()
        .setNumDatanodes(3)
        .setNumDataVolumes(3)
        .setDatanodeStorageType(Collections.nCopies(3,
            Arrays.asList(StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE)))
        .addService(s3g)
        .build();
  }

  @Test
  void putCopyAndMultipartUploadSendStoragePolicy() throws Exception {
    String bucketName = "storage-class-write";
    try (S3Client s3 = new S3ClientFactory(s3g.getConf()).createS3ClientV2()) {
      s3.createBucket(b -> b.bucket(bucketName));

      s3.putObject(b -> b.bucket(bucketName).key("put").storageClass(StorageClass.GLACIER),
          RequestBody.fromString(CONTENT));
      s3.putObject(b -> b.bucket(bucketName).key("no-header"), RequestBody.fromString(CONTENT));
      s3.copyObject(b -> b.sourceBucket(bucketName).sourceKey("no-header")
          .destinationBucket(bucketName).destinationKey("copy").storageClass(StorageClass.GLACIER));

      String uploadId = s3.createMultipartUpload(b -> b.bucket(bucketName).key("mpu")
          .storageClass(StorageClass.GLACIER)).uploadId();
      UploadPartResponse part = s3.uploadPart(b -> b.bucket(bucketName).key("mpu").uploadId(uploadId).partNumber(1),
          RequestBody.fromString(CONTENT));
      s3.completeMultipartUpload(b -> b.bucket(bucketName).key("mpu").uploadId(uploadId)
          .multipartUpload(CompletedMultipartUpload.builder()
              .parts(CompletedPart.builder().partNumber(1).eTag(part.eTag()).build()).build()));
    }

    try (OzoneClient client = getCluster().newClient()) {
      OzoneBucket bucket = client.getObjectStore().getS3Bucket(bucketName);
      for (String key : new String[] {"put", "copy", "mpu"}) {
        OzoneKeyDetails keyDetails = bucket.getKey(key);
        assertEquals(OzoneStoragePolicy.COLD, keyDetails.getStoragePolicy(), key);
        assertEquals(RATIS_THREE, keyDetails.getReplicationConfig(), key);
      }
      // No header sends no policy, so OM records the bucket policy or the cluster default.
      OzoneKeyDetails noHeader = bucket.getKey("no-header");
      assertEquals(bucket.getStoragePolicy() != null ? bucket.getStoragePolicy()
          : OzoneStoragePolicy.getDefaultPolicy(), noHeader.getStoragePolicy());
    }
  }
}
