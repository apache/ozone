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
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.snapshot.OMSnapshotMoveTableKeysResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.upgrade.DisallowedUntilLayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveTableKeysRequest;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
      SnapshotInfo nextSnapshot = SnapshotUtils.getNextSnapshot(ozoneManager, snapshotChainManager, fromSnapshot);

      // If next snapshot is not active then ignore move. Since this could be a redundant operations.
      if (nextSnapshot != null && nextSnapshot.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE) {
        throw new OMException("Next snapshot : " + nextSnapshot + " in chain is not active.",
            OMException.ResultCodes.INVALID_SNAPSHOT_ERROR);
      }

      String expectedBucketKeyPrefix = omMetadataManager.getBucketKeyPrefix(fromSnapshot.getVolumeName(),
          fromSnapshot.getBucketName());
      String expectedBucketKeyPrefixFSO = omMetadataManager.getBucketKeyPrefixFSO(fromSnapshot.getVolumeName(),
          fromSnapshot.getBucketName());

      // Filter only deleted keys with atlest one keyInfo per key.
      Set<String> keys = new HashSet<>();
      List<SnapshotMoveKeyInfos> deletedKeys =
          moveTableKeysRequest.getDeletedKeysList().stream()
              .filter(snapshotMoveKeyInfos -> !snapshotMoveKeyInfos.getKeyInfosList().isEmpty())
              .collect(Collectors.toList());
      //validate deleted key starts with bucket prefix.[/<volName>/<bucketName>/]
      for (SnapshotMoveKeyInfos deletedKey : deletedKeys) {
        if (!deletedKey.getKey().startsWith(expectedBucketKeyPrefix)) {
          throw new OMException("Deleted Key: " + deletedKey + " doesn't start with prefix " + expectedBucketKeyPrefix,
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
        if (keys.contains(deletedKey.getKey())) {
          throw new OMException("Duplicate Deleted Key: " + deletedKey + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
        } else {
          keys.add(deletedKey.getKey());
        }
      }
      keys.clear();
      List<HddsProtos.KeyValue> renamedKeysList = moveTableKeysRequest.getRenamedKeysList().stream()
          .filter(keyValue -> keyValue.hasKey() && keyValue.hasValue()).collect(Collectors.toList());
      //validate rename key starts with bucket prefix.[/<volName>/<bucketName>/]
      for (HddsProtos.KeyValue renamedKey : renamedKeysList) {
        if (!renamedKey.getKey().startsWith(expectedBucketKeyPrefix)) {
          throw new OMException("Rename Key: " + renamedKey + " doesn't start with prefix " + expectedBucketKeyPrefix,
              OMException.ResultCodes.INVALID_KEY_NAME);
        }
        if (keys.contains(renamedKey.getKey())) {
          throw new OMException("Duplicate rename Key: " + renamedKey + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
        } else {
          keys.add(renamedKey.getKey());
        }
      }
      keys.clear();
      // Filter only deleted dirs with only one keyInfo per key.
      List<SnapshotMoveKeyInfos> deletedDirs = moveTableKeysRequest.getDeletedDirsList().stream()
          .filter(snapshotMoveKeyInfos -> snapshotMoveKeyInfos.getKeyInfosList().size() == 1)
          .collect(Collectors.toList());

      //validate deleted key starts with bucket FSO path prefix.[/<volId>/<bucketId>/]
      for (SnapshotMoveKeyInfos deletedDir : deletedDirs) {
        if (!deletedDir.getKey().startsWith(expectedBucketKeyPrefixFSO)) {
          throw new OMException("Deleted dir: " + deletedDir + " doesn't start with prefix " +
              expectedBucketKeyPrefixFSO, OMException.ResultCodes.INVALID_KEY_NAME);
        }
        if (keys.contains(deletedDir.getKey())) {
          throw new OMException("Duplicate rename Key: " + deletedDir + " in request",
              OMException.ResultCodes.INVALID_REQUEST);
        } else {
          keys.add(deletedDir.getKey());
        }
      }

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

