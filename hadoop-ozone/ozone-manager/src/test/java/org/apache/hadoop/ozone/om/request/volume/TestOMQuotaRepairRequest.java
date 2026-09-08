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

package org.apache.hadoop.ozone.om.request.volume;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMQuotaRepairResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketQuotaCount;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QuotaRepairRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Tests quota repair request.
 */
public class TestOMQuotaRepairRequest extends OMVolumeRequestTests {

  @Test
  public void testRepairDurableWriteNotAffectedByLaterBucketCacheMutation() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "bucket1";
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo driftedBucket = omMetadataManager.getBucketTable().get(bucketKey)
        .toBuilder()
        .setUsedBytes(1)
        .build();
    omMetadataManager.getBucketTable().put(bucketKey, driftedBucket);
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey), CacheValue.get(1L, driftedBucket));

    OMRequest omRequest = OMRequest.newBuilder()
        .setClientId("test-client")
        .setCmdType(Type.QuotaRepair)
        .setQuotaRepairRequest(QuotaRepairRequest.newBuilder()
            .addBucketCount(BucketQuotaCount.newBuilder()
                .setVolName(volumeName)
                .setBucketName(bucketName)
                .setDiffUsedBytes(-1)
                .setDiffUsedNamespace(0)
                .setSupportOldQuota(false)
                .build())
            .setSupportVolumeOldQuota(false)
            .build())
        .build();

    OMQuotaRepairRequest omQuotaRepairRequest = new OMQuotaRepairRequest(omRequest);
    OMClientResponse omClientResponse =
        omQuotaRepairRequest.validateAndUpdateCache(ozoneManager, 2L);
    assertThat(omClientResponse.getOMResponse().getStatus()).isEqualTo(Status.OK);

    OmBucketInfo cachedBucket =
        OMKeyRequest.getBucketInfo(omMetadataManager, volumeName, bucketName);
    assertThat(cachedBucket).isNotNull();
    assertThat(cachedBucket.getUsedBytes()).isEqualTo(0);

    cachedBucket.incrUsedBytes(1);
    assertThat(cachedBucket.getUsedBytes()).isEqualTo(1);

    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    ((OMQuotaRepairResponse) omClientResponse).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmBucketInfo durableBucket = omMetadataManager.getBucketTable().getSkipCache(bucketKey);
    assertThat(durableBucket.getUsedBytes()).isEqualTo(0);
  }
}
