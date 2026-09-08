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

package org.apache.hadoop.ozone.om.snapshot.trapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.snapshot.SnapshotRequestAndResponseTests;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BucketDeletedDataCalculator}.
 */
public class TestBucketDeletedDataCalculator extends SnapshotRequestAndResponseTests {

  @Test
  public void testAosDeletedKeyReportedAsPurgeable() throws Exception {
    String volume = getVolumeName();
    String bucket = getBucketName();
    long bucketId = getOmMetadataManager().getBucketId(volume, bucket);

    KeyManager keyManager = mock(KeyManager.class);
    when(keyManager.getMetadataManager()).thenReturn(getOmMetadataManager());
    when(keyManager.getDeletedDirEntries(volume, bucket)).thenAnswer(invocation -> {
      Table<String, OmKeyInfo> deletedDirTable = getOmMetadataManager().getDeletedDirTable();
      String prefix = getOmMetadataManager().getTableBucketPrefix(
          deletedDirTable.getName(), volume, bucket);
      return deletedDirTable.iterator(prefix);
    });
    when(getOzoneManager().getKeyManager()).thenReturn(keyManager);

    BucketManager bucketManager = mock(BucketManager.class);
    String bucketDbKey = getOmMetadataManager().getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = getOmMetadataManager().getBucketTable().get(bucketDbKey);
    when(bucketManager.getBucketInfo(volume, bucket)).thenReturn(bucketInfo);
    when(getOzoneManager().getBucketManager()).thenReturn(bucketManager);

    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
            volume, bucket, "key-a",
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .setObjectID(101L)
        .setUpdateID(101L)
        .build();
    RepeatedOmKeyInfo repeated = new RepeatedOmKeyInfo(keyInfo, bucketId);

    String deletedDbKey = getOmMetadataManager().getOzoneKey(volume, bucket, keyInfo.getKeyName());
    try (BatchOperation batch = getOmMetadataManager().getStore().initBatchOperation()) {
      getOmMetadataManager().getDeletedTable().putWithBatch(batch, deletedDbKey, repeated);
      getOmMetadataManager().getStore().commitBatchOperation(batch);
    }

    BucketDeletedDataCalculator.BucketDeletedBytesStats stats =
        new BucketDeletedDataCalculator(getOzoneManager())
            .calculate(volume, bucket);

    assertEquals(0L, stats.getSnapshotTrappedBytes());
    assertEquals(0L, stats.getSnapshotTrappedKeys());
    assertEquals(0L, stats.getSnapshotTrappedDirs());
    assertTrue(stats.getPurgeableBytes() > 0L);
    assertEquals(1L, stats.getPurgeableKeys());
    assertEquals(0L, stats.getPurgeableDirs());
  }
}

