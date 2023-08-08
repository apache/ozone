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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveDeletedKeysResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

/**
 * Handles OMSnapshotMoveDeletedKeys Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotMoveDeletedKeysRequest extends OMClientRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMSnapshotMoveDeletedKeysRequest.class);

  public OMSnapshotMoveDeletedKeysRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    OmSnapshotManager omSnapshotManager = ozoneManager.getOmSnapshotManager();
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl)
        ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager =
        omMetadataManager.getSnapshotChainManager();

    SnapshotMoveDeletedKeysRequest moveDeletedKeysRequest =
        getOmRequest().getSnapshotMoveDeletedKeysRequest();
    SnapshotInfo fromSnapshot = SnapshotInfo.getFromProtobuf(
        moveDeletedKeysRequest.getFromSnapshot());

    // If there is no Non-Deleted Snapshot move the
    // keys to Active Object Store.
    SnapshotInfo nextSnapshot = null;
    OMClientResponse omClientResponse = null;
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OmResponseUtil.getOMResponseBuilder(getOmRequest());
    try {
      nextSnapshot = SnapshotUtils.getNextActiveSnapshot(fromSnapshot,
          snapshotChainManager, omSnapshotManager);

      // Get next non-deleted snapshot.
      List<SnapshotMoveKeyInfos> nextDBKeysList =
          moveDeletedKeysRequest.getNextDBKeysList();
      List<SnapshotMoveKeyInfos> reclaimKeysList =
          moveDeletedKeysRequest.getReclaimKeysList();
      List<HddsProtos.KeyValue> renamedKeysList =
          moveDeletedKeysRequest.getRenamedKeysList();
      List<String> movedDirs =
          moveDeletedKeysRequest.getDeletedDirsToMoveList();

      omClientResponse = new OMSnapshotMoveDeletedKeysResponse(
          omResponse.build(), fromSnapshot, nextSnapshot,
          nextDBKeysList, reclaimKeysList, renamedKeysList, movedDirs);

    } catch (IOException ex) {
      omClientResponse = new OMSnapshotMoveDeletedKeysResponse(
          createErrorOMResponse(omResponse, ex));
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
    }

    return omClientResponse;
  }
}

