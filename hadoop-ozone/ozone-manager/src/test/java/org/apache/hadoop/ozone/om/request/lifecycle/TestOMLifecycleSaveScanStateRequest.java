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

package org.apache.hadoop.ozone.om.request.lifecycle;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleScanState;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManagerTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleScanState;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SaveLifecycleScanStateRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests OMLifecycleSaveScanStateRequest.
 */
public class TestOMLifecycleSaveScanStateRequest {

  private static final String BUCKET_KEY = "/vol1/bucket1";
  private static final long BUCKET_OBJ_ID = 100L;
  private static final long LCC_UPDATE_ID = 10L;

  @Test
  public void testPreExecuteAdminCheck() throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMVersionManager versionManager = OMVersionManagerTestUtils.mockPreFinalizedOmVersionManager();
    when(ozoneManager.getVersionManager()).thenReturn(versionManager);
    
    // Test when admin authorization is enabled but user is not admin
    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(true);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
        .setClientId(UUID.randomUUID().toString())
        .setSaveLifecycleScanStateRequest(SaveLifecycleScanStateRequest.newBuilder()
            .setState(LifecycleScanState.newBuilder().setBucketKey("dummy").setScanStartTime(1L).build())
            .build())
        .build();

    OMLifecycleSaveScanStateRequest request = new OMLifecycleSaveScanStateRequest(omRequest);
    request.setUGI(UserGroupInformation.getCurrentUser());

    OMException exception = assertThrows(OMException.class, () -> {
      request.preExecute(ozoneManager);
    });

    assertEquals(OMException.ResultCodes.ACCESS_DENIED, exception.getResult());
    assertTrue(exception.getMessage().contains("Superuser privilege is required"));
    
    // Test when user is admin
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(true);
    assertDoesNotThrow(() -> request.preExecute(ozoneManager));

    // Test when admin authorization is disabled
    when(ozoneManager.isAdminAuthorizationEnabled()).thenReturn(false);
    when(ozoneManager.isAdmin(any(UserGroupInformation.class))).thenReturn(false);
    assertDoesNotThrow(() -> request.preExecute(ozoneManager));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    Table<String, OmLifecycleScanState> table = mock(Table.class);
    OMClientResponse response = saveScanState(table, BUCKET_OBJ_ID, LCC_UPDATE_ID);
    assertEquals(OzoneManagerProtocolProtos.Status.OK, response.getOMResponse().getStatus());
    verify(table).addCacheEntry(any(CacheKey.class), any(CacheValue.class));
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, false"})
  public void testSkipScanStateOfDeletedBucketOrLifecycle(boolean bucketExists, boolean lccExists) throws Exception {
    Table<String, OmLifecycleScanState> table = mock(Table.class);
    OMClientResponse response = saveScanState(table, bucketExists ? BUCKET_OBJ_ID : null,
        lccExists ? LCC_UPDATE_ID : null);
    assertEquals(OzoneManagerProtocolProtos.Status.LIFECYCLE_CONFIGURATION_NOT_FOUND,
        response.getOMResponse().getStatus());
    verify(table, never()).addCacheEntry(any(CacheKey.class), any(CacheValue.class));

    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    when(omMetadataManager.getLifecycleScanStateTable()).thenReturn(table);
    response.checkAndUpdateDB(omMetadataManager, mock(BatchOperation.class));
    verify(table, never()).putWithBatch(any(), any(), any());
  }

  @ParameterizedTest
  @CsvSource({"101, 10", "100, 11"})
  public void testSkipScanStateOfRecreatedBucketOrLifecycle(long bucketObjID, long lccUpdateID) throws Exception {
    Table<String, OmLifecycleScanState> table = mock(Table.class);
    OMClientResponse response = saveScanState(table, bucketObjID, lccUpdateID);
    assertEquals(OzoneManagerProtocolProtos.Status.LIFECYCLE_CONFIGURATION_NOT_FOUND,
        response.getOMResponse().getStatus());
    verify(table, never()).addCacheEntry(any(CacheKey.class), any(CacheValue.class));
  }

  private OMClientResponse saveScanState(Table<String, OmLifecycleScanState> scanStateTable, Long bucketObjID,
      Long lccUpdateID) throws Exception {
    OzoneManager ozoneManager = mock(OzoneManager.class);
    OMMetadataManager omMetadataManager = mock(OMMetadataManager.class);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    when(omMetadataManager.getLifecycleScanStateTable()).thenReturn(scanStateTable);

    Table<String, OmBucketInfo> bucketTable = mock(Table.class);
    when(omMetadataManager.getBucketTable()).thenReturn(bucketTable);
    if (bucketObjID != null) {
      when(bucketTable.get(BUCKET_KEY)).thenReturn(OmBucketInfo.newBuilder()
          .setVolumeName("vol1").setBucketName("bucket1").setObjectID(bucketObjID).build());
    }
    Table<String, OmLifecycleConfiguration> lccTable = mock(Table.class);
    when(omMetadataManager.getLifecycleConfigurationTable()).thenReturn(lccTable);
    if (lccUpdateID != null) {
      when(lccTable.get(BUCKET_KEY)).thenReturn(new OmLifecycleConfiguration.Builder()
          .setVolume("vol1")
          .setBucket("bucket1")
          .setBucketLayout(BucketLayout.OBJECT_STORE)
          .addRule(new OmLCRule.Builder().setPrefix("")
              .addAction(new OmLCExpiration.Builder().setDays(30).build())
              .build())
          .setCreationTime(Time.now())
          .setObjectID(1L)
          .setUpdateID(lccUpdateID)
          .build());
    }

    LifecycleScanState stateProto = LifecycleScanState.newBuilder()
        .setBucketKey(BUCKET_KEY)
        .setBucketObjID(BUCKET_OBJ_ID)
        .setLifecycleConfigurationUpdateID(LCC_UPDATE_ID)
        .setScanStartTime(123456789L)
        .setLastScannedKey("key1")
        .build();

    SaveLifecycleScanStateRequest saveRequest = SaveLifecycleScanStateRequest.newBuilder()
        .setState(stateProto)
        .build();

    OMRequest omRequest = OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SaveLifecycleScanState)
        .setClientId(UUID.randomUUID().toString())
        .setSaveLifecycleScanStateRequest(saveRequest)
        .build();

    OMLifecycleSaveScanStateRequest request = new OMLifecycleSaveScanStateRequest(omRequest);
    OMClientResponse response = request.validateAndUpdateCache(ozoneManager, 100L);
    assertNotNull(response);
    return response;
  }
}
