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

package org.apache.hadoop.ozone.om.request.snapshot;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.INTERNAL_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotSetPropertyResponse;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetSnapshotPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests TestOMSnapshotSetPropertyRequest
 * TestOMSnapshotSetPropertyResponse class.
 */
public class TestOMSnapshotSetPropertyRequestAndResponse extends TestSnapshotRequestAndResponse {
  private String snapName;
  private long exclusiveSize;
  private long exclusiveSizeAfterRepl;

  @BeforeEach
  void setup() {
    snapName = UUID.randomUUID().toString();
    exclusiveSize = 2000L;
    exclusiveSizeAfterRepl = 6000L;
  }

  @Test
  public void testValidateAndUpdateCache() throws IOException {
    long initialSnapshotSetPropertyCount = getOmSnapshotIntMetrics().getNumSnapshotSetProperties();
    long initialSnapshotSetPropertyFailCount = getOmSnapshotIntMetrics().getNumSnapshotSetPropertyFails();

    createSnapshotDataForTest();
    assertFalse(getOmMetadataManager().getSnapshotInfoTable().isEmpty());
    List<OMRequest> snapshotUpdateSizeRequests =
        createSnapshotUpdateSizeRequest();

    // Pre-Execute
    for (OMRequest request: snapshotUpdateSizeRequests) {
      OMSnapshotSetPropertyRequest omSnapshotSetPropertyRequest = new
          OMSnapshotSetPropertyRequest(request);
      OMRequest modifiedOmRequest = omSnapshotSetPropertyRequest
          .preExecute(getOzoneManager());
      omSnapshotSetPropertyRequest = new
          OMSnapshotSetPropertyRequest(modifiedOmRequest);

      // Validate and Update Cache
      OMSnapshotSetPropertyResponse omSnapshotSetPropertyResponse =
          (OMSnapshotSetPropertyResponse) omSnapshotSetPropertyRequest
              .validateAndUpdateCache(getOzoneManager(), 200L);

      // Commit to DB.
      omSnapshotSetPropertyResponse.checkAndUpdateDB(getOmMetadataManager(),
          getBatchOperation());
      getOmMetadataManager().getStore().commitBatchOperation(getBatchOperation());
    }

    assertEquals(initialSnapshotSetPropertyCount + snapshotUpdateSizeRequests.size(),
        getOmSnapshotIntMetrics().getNumSnapshotSetProperties());
    assertEquals(initialSnapshotSetPropertyFailCount, getOmSnapshotIntMetrics().getNumSnapshotSetPropertyFails());
    // Check if the exclusive size is set.
    try (Table.KeyValueIterator<String, SnapshotInfo>
             iterator = getOmMetadataManager().getSnapshotInfoTable().iterator()) {
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

  /**
   * This test is mainly to validate metrics and error code.
   */
  @Test
  public void testValidateAndUpdateCacheFailure() throws IOException {
    long initialSnapshotSetPropertyCount = getOmSnapshotIntMetrics().getNumSnapshotSetProperties();
    long initialSnapshotSetPropertyFailCount = getOmSnapshotIntMetrics().getNumSnapshotSetPropertyFails();

    createSnapshotDataForTest();
    assertFalse(getOmMetadataManager().getSnapshotInfoTable().isEmpty());
    List<OMRequest> snapshotUpdateSizeRequests = createSnapshotUpdateSizeRequest();

    OmMetadataManagerImpl mockedMetadataManager = mock(OmMetadataManagerImpl.class);
    Table<String, SnapshotInfo> mockedSnapshotInfoTable = mock(Table.class);

    when(mockedSnapshotInfoTable.get(anyString())).thenThrow(new CodecException("Injected fault error."));
    when(mockedMetadataManager.getSnapshotInfoTable()).thenReturn(mockedSnapshotInfoTable);
    when(getOzoneManager().getMetadataManager()).thenReturn(mockedMetadataManager);

    for (OMRequest omRequest: snapshotUpdateSizeRequests) {
      OMSnapshotSetPropertyRequest omSnapshotSetPropertyRequest = new OMSnapshotSetPropertyRequest(omRequest);
      OMRequest modifiedOmRequest = omSnapshotSetPropertyRequest.preExecute(getOzoneManager());
      omSnapshotSetPropertyRequest = new OMSnapshotSetPropertyRequest(modifiedOmRequest);

      // Validate and Update Cache
      OMSnapshotSetPropertyResponse omSnapshotSetPropertyResponse = (OMSnapshotSetPropertyResponse)
          omSnapshotSetPropertyRequest.validateAndUpdateCache(getOzoneManager(), 200L);

      assertEquals(INTERNAL_ERROR, omSnapshotSetPropertyResponse.getOMResponse().getStatus());
    }

    assertEquals(initialSnapshotSetPropertyCount, getOmSnapshotIntMetrics().getNumSnapshotSetProperties());
    assertEquals(initialSnapshotSetPropertyFailCount + snapshotUpdateSizeRequests.size(),
        getOmSnapshotIntMetrics().getNumSnapshotSetPropertyFails());
  }

  private void assertCacheValues(String dbKey) {
    CacheValue<SnapshotInfo> cacheValue = getOmMetadataManager()
        .getSnapshotInfoTable()
        .getCacheValue(new CacheKey<>(dbKey));
    assertEquals(exclusiveSize, cacheValue.getCacheValue().getExclusiveSize());
    assertEquals(exclusiveSizeAfterRepl, cacheValue.getCacheValue()
        .getExclusiveReplicatedSize());
  }

  private List<OMRequest> createSnapshotUpdateSizeRequest()
      throws IOException {
    List<OMRequest> omRequests = new ArrayList<>();
    try (Table.KeyValueIterator<String, SnapshotInfo>
             iterator = getOmMetadataManager().getSnapshotInfoTable().iterator()) {
      while (iterator.hasNext()) {
        String snapDbKey = iterator.next().getKey();
        SnapshotSize snapshotSize = SnapshotSize.newBuilder()
            .setExclusiveSize(exclusiveSize)
            .setExclusiveReplicatedSize(exclusiveSizeAfterRepl)
            .build();
        SetSnapshotPropertyRequest snapshotUpdateSizeRequest =
            SetSnapshotPropertyRequest.newBuilder()
                .setSnapshotKey(snapDbKey)
                .setSnapshotSize(snapshotSize)
                .build();

        OMRequest omRequest = OMRequest.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.SetSnapshotProperty)
            .setSetSnapshotPropertyRequest(snapshotUpdateSizeRequest)
            .setClientId(UUID.randomUUID().toString())
            .build();
        omRequests.add(omRequest);
      }
    }
    return omRequests;
  }

  private void createSnapshotDataForTest() throws IOException {
    // Create 10 Snapshots
    for (int i = 0; i < 10; i++) {
      OMRequestTestUtils.addSnapshotToTableCache(getVolumeName(), getBucketName(),
          snapName + i, getOmMetadataManager());
    }
  }
}
