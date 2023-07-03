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

import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotPurgeResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotPurgeRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles OMSnapshotPurge Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotPurgeRequest extends OMClientRequest {

  public OMSnapshotPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    OmSnapshotManager omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();

    OMClientResponse omClientResponse = null;

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    SnapshotPurgeRequest snapshotPurgeRequest = getOmRequest()
        .getSnapshotPurgeRequest();

    try {
      List<String> snapshotDbKeys = snapshotPurgeRequest
          .getSnapshotDBKeysList();
      List<String> snapInfosToUpdate = snapshotPurgeRequest
          .getUpdatedSnapshotDBKeyList();
      Map<String, SnapshotInfo> updatedSnapInfos = new HashMap<>();

      // Snapshots that are already deepCleaned by the KeyDeletingService
      // can be marked as deepCleaned.
      for (String snapTableKey : snapInfosToUpdate) {
        SnapshotInfo snapInfo = omMetadataManager.getSnapshotInfoTable()
            .get(snapTableKey);

        updateSnapshotInfoAndCache(snapInfo, omMetadataManager,
            trxnLogIndex, updatedSnapInfos, false);
      }

      // Snapshots that are purged by the SnapshotDeletingService
      // will update the next snapshot so that is can be deep cleaned
      // by the KeyDeletingService in the next run.
      for (String snapTableKey : snapshotDbKeys) {
        SnapshotInfo fromSnapshot = omMetadataManager.getSnapshotInfoTable()
            .get(snapTableKey);

        SnapshotInfo nextSnapshot = SnapshotUtils
            .getNextActiveSnapshot(fromSnapshot,
                snapshotChainManager, omSnapshotManager);
        updateSnapshotInfoAndCache(nextSnapshot, omMetadataManager,
            trxnLogIndex, updatedSnapInfos, true);
      }

      omClientResponse = new OMSnapshotPurgeResponse(omResponse.build(),
          snapshotDbKeys, updatedSnapInfos);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotPurgeResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    return omClientResponse;
  }

  private void updateSnapshotInfoAndCache(SnapshotInfo snapInfo,
      OmMetadataManagerImpl omMetadataManager, long trxnLogIndex,
      Map<String, SnapshotInfo> updatedSnapInfos, boolean deepClean) {
    if (snapInfo != null) {
      snapInfo.setDeepClean(deepClean);

      // Update table cache first
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(
          new CacheKey<>(snapInfo.getTableKey()),
          CacheValue.get(trxnLogIndex, snapInfo));
      updatedSnapInfos.put(snapInfo.getTableKey(), snapInfo);
    }
  }
}
