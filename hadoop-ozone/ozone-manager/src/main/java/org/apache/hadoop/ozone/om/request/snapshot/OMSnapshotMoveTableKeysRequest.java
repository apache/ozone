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
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveDeletedKeysResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveTableKeysResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveDeletedKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature.FILESYSTEM_SNAPSHOT;

/**
 * Handles OMSnapshotMoveTableKeysRequest Request.
 * This is an OM internal request. Does not need @RequireSnapshotFeatureState.
 */
public class OMSnapshotMoveTableKeysRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMSnapshotMoveTableKeysRequest.class);

  public OMSnapshotMoveTableKeysRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  @DisallowedUntilLayoutVersion(FILESYSTEM_SNAPSHOT)
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();
    SnapshotChainManager snapshotChainManager = omMetadataManager.getSnapshotChainManager();

    SnapshotMoveTableKeysRequest moveTableKeysRequest = getOmRequest().getSnapshotMoveTableKeysRequest();

    OMClientResponse omClientResponse;
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(getOmRequest());
    try {
      SnapshotInfo fromSnapshot = SnapshotUtils.getSnapshotInfo(ozoneManager,
          snapshotChainManager, fromProtobuf(moveTableKeysRequest.getFromSnapshotID()));
      // If there is no Non-Deleted Snapshot move the
      // keys to Active Object Store.
      SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager,fromSnapshot);

      // If next snapshot is not active then ignore move. Since this could be a redundant operations.
      if (nextSnapshot != null && nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
        throw new OMException("Next snapshot : " + nextSnapshot + " in chain is not active.",
            OMException.ResultCodes.INVALID_REQUEST);
      }

      // Filter only deleted keys with atlest one keyInfo per key.
      List<SnapshotMoveKeyInfos> deletedKeys =
          moveTableKeysRequest.getDeletedKeysList().stream()
              .filter(snapshotMoveKeyInfos -> !snapshotMoveKeyInfos.getKeyInfosList().isEmpty())
              .collect(Collectors.toList());
      List<HddsProtos.KeyValue> renamedKeysList = moveTableKeysRequest.getRenamedKeysList();
      // Filter only deleted dirs with only one keyInfo per key.
      List<SnapshotMoveKeyInfos> deletedDirs = moveTableKeysRequest.getDeletedDirsList().stream()
          .filter(snapshotMoveKeyInfos -> snapshotMoveKeyInfos.getKeyInfosList().size() == 1)
          .collect(Collectors.toList());

      // Update lastTransactionInfo for fromSnapshot and the nextSnapshot.
      fromSnapshot.setLastTransactionInfo(TransactionInfo.valueOf(termIndex).toByteString());
      omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshot.getTableKey()),
          CacheValue.get(termIndex.getIndex(), fromSnapshot));
      if (nextSnapshot != null) {
        nextSnapshot.setLastTransactionInfo(TransactionInfo.valueOf(termIndex).toByteString());
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(nextSnapshot.getTableKey()),
            CacheValue.get(termIndex.getIndex(), nextSnapshot));
      }
      omClientResponse = new OMSnapshotMoveTableKeysResponse(omResponse.build(), fromSnapshot, nextSnapshot,
          deletedKeys, deletedDirs, renamedKeysList);
    } catch (IOException ex) {
      omClientResponse = new OMSnapshotMoveTableKeysResponse(createErrorOMResponse(omResponse, ex));
    }

    return omClientResponse;
  }
}

