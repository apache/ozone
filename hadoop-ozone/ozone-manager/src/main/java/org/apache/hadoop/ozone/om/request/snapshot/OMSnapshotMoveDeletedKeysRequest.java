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

import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveDeletedKeysResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;

/**
 * Handles OMSnapshotMoveDeletedKeys Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotMoveDeletedKeysRequest extends OMClientRequest {

  public OMSnapshotMoveDeletedKeysRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, ExecutionContext context) {
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
      // Check the snapshot exists.
      SnapshotInfo snapshotInfo = SnapshotUtils.getSnapshotInfo(ozoneManager, fromSnapshot.getTableKey());

      nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager, snapshotInfo);

      // Get next non-deleted snapshot.
      List<SnapshotMoveKeyInfos> nextDBKeysList = moveDeletedKeysRequest.getNextDBKeysList();
      List<SnapshotMoveKeyInfos> reclaimKeysList = moveDeletedKeysRequest.getReclaimKeysList();
      List<HddsProtos.KeyValue> renamedKeysList = moveDeletedKeysRequest.getRenamedKeysList();
      List<String> movedDirs = moveDeletedKeysRequest.getDeletedDirsToMoveList();
      OmBucketInfo omBucketInfo = OMKeyRequest.getBucketInfo(omMetadataManager, snapshotInfo.getVolumeName(),
          snapshotInfo.getBucketName());
      OMSnapshotMoveUtils.updateCache(ozoneManager, fromSnapshot, nextSnapshot, context);
      omClientResponse = new OMSnapshotMoveDeletedKeysResponse.Builder()
          .setOmResponse(omResponse.build())
          .setFromSnapshot(fromSnapshot)
          .setNextSnapshot(nextSnapshot)
          .setNextDBKeysList(nextDBKeysList)
          .setReclaimKeysList(reclaimKeysList)
          .setRenamedKeysList(renamedKeysList)
          .setMovedDirs(movedDirs)
          .setBucketId(omBucketInfo.getObjectID())
          .build();

    } catch (IOException ex) {
      omClientResponse = new OMSnapshotMoveDeletedKeysResponse(
          createErrorOMResponse(omResponse, ex));
    }

    return omClientResponse;
  }
}

