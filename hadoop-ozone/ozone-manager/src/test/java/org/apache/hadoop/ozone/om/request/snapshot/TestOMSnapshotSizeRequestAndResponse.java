/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;

import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotUpdateSizeResponse;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotUpdateSizeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests TestOMSnapshotSizeRequest TestOMSnapshotSizeResponse class.
 */
public class TestOMSnapshotSizeRequestAndResponse {
  private BatchOperation batchOperation;
  private OzoneManager ozoneManager;
  private OMMetadataManager omMetadataManager;

  private String volumeName;
  private String bucketName;
  private String snapName;
  private long exclusiveSize;
  private long exclusiveSizeAfterRepl;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private static final OzoneManagerDoubleBufferHelper
      DOUBLE_BUFFER_HELPER = ((response, transactionIndex) -> null);

  @BeforeEach
  void setup(@TempDir File testDir) throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.isAllowed(anyString())).thenReturn(true);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.isRatisEnabled()).thenReturn(true);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        testDir.getAbsolutePath());
    ozoneConfiguration.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);

    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    snapName = UUID.randomUUID().toString();
    exclusiveSize = 2000L;
    exclusiveSizeAfterRepl = 6000L;
  }

  @Test
  public void testValidateAndUpdateCache() throws IOException {
    createSnapshotDataForTest();
    assertFalse(omMetadataManager.getSnapshotInfoTable().isEmpty());
    OzoneManagerProtocolProtos.OMRequest snapshotUpdateSizeRequest =
        createSnapshotUpdateSizeRequest();

    // Pre-Execute
    OMSnapshotUpdateSizeRequest omSnapshotUpdateSizeRequest = new
        OMSnapshotUpdateSizeRequest(snapshotUpdateSizeRequest);
    OMRequest modifiedOmRequest = omSnapshotUpdateSizeRequest
        .preExecute(ozoneManager);
    omSnapshotUpdateSizeRequest = new
        OMSnapshotUpdateSizeRequest(modifiedOmRequest);

    // Validate and Update Cache
    OMSnapshotUpdateSizeResponse omSnapshotUpdateSizeResponse =
        (OMSnapshotUpdateSizeResponse) omSnapshotUpdateSizeRequest
            .validateAndUpdateCache(ozoneManager, 200L,
            DOUBLE_BUFFER_HELPER);

    // Commit to DB.
    batchOperation = omMetadataManager.getStore().initBatchOperation();
    omSnapshotUpdateSizeResponse.checkAndUpdateDB(omMetadataManager,
        batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Check if the exclusive size is set.
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
             iterator = omMetadataManager.getSnapshotInfoTable().iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> snapshotEntry = iterator.next();
        assertCacheValues(snapshotEntry.getKey());
        assertEquals(exclusiveSize, snapshotEntry.getValue().
            getExclusiveSize());
        assertEquals(exclusiveSizeAfterRepl, snapshotEntry.getValue()
            .getExclusiveReplicatedSize());
      }
    }
  }

  private void assertCacheValues(String dbKey) {
    CacheValue<SnapshotInfo> cacheValue = omMetadataManager
        .getSnapshotInfoTable()
        .getCacheValue(new CacheKey<>(dbKey));
    assertEquals(exclusiveSize, cacheValue.getCacheValue().getExclusiveSize());
    assertEquals(exclusiveSizeAfterRepl, cacheValue.getCacheValue()
        .getExclusiveReplicatedSize());
  }

  private OMRequest createSnapshotUpdateSizeRequest() throws IOException {
    List<SnapshotSize> snapshotSizeList = new ArrayList<>();
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
             iterator = omMetadataManager.getSnapshotInfoTable().iterator()) {
      while (iterator.hasNext()) {
        String snapDbKey = iterator.next().getKey();
        SnapshotSize snapshotSize = SnapshotSize.newBuilder()
            .setSnapshotKey(snapDbKey)
            .setExclusiveSize(exclusiveSize)
            .setExclusiveReplicatedSize(exclusiveSizeAfterRepl)
            .build();
        snapshotSizeList.add(snapshotSize);
      }
    }
    SnapshotUpdateSizeRequest snapshotUpdateSizeRequest =
        SnapshotUpdateSizeRequest.newBuilder()
            .addAllSnapshotSize(snapshotSizeList)
            .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotUpdateSize)
        .setSnapshotUpdateSizeRequest(snapshotUpdateSizeRequest)
        .setClientId(UUID.randomUUID().toString())
        .build();

    return omRequest;
  }

  private void createSnapshotDataForTest() throws IOException {
    // Create 10 Snapshots
    for (int i = 0; i < 10; i++) {
      OMRequestTestUtils.addSnapshotToTableCache(volumeName, bucketName,
          snapName + i, omMetadataManager);
    }
  }
}
