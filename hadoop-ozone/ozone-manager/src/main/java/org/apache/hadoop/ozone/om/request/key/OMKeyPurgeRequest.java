/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyPurgeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeletedKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgeKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SnapshotMoveKeyInfos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.HddsUtils.fromProtobuf;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.validatePreviousSnapshotId;

/**
 * Handles purging of keys from OM DB.
 */
public class OMKeyPurgeRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyPurgeRequest.class);

  public OMKeyPurgeRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager, TermIndex termIndex) {
    PurgeKeysRequest purgeKeysRequest = getOmRequest().getPurgeKeysRequest();
    List<DeletedKeys> bucketDeletedKeysList = purgeKeysRequest.getDeletedKeysList();
    List<SnapshotMoveKeyInfos> keysToUpdateList = purgeKeysRequest.getKeysToUpdateList();
    String fromSnapshot = purgeKeysRequest.hasSnapshotTableKey() ?
        purgeKeysRequest.getSnapshotTableKey() : null;
    OmMetadataManagerImpl omMetadataManager = (OmMetadataManagerImpl) ozoneManager.getMetadataManager();

    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    final SnapshotInfo fromSnapshotInfo;

    try {
      fromSnapshotInfo = fromSnapshot != null ? null : SnapshotUtils.getSnapshotInfo(ozoneManager, fromSnapshot);
    } catch (IOException ex) {
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, ex));
    }

    List<String> keysToBePurgedList = new ArrayList<>();
    List<String> renamedKeysToBePurged = new ArrayList<>();

    Map<Pair<String, String>, Boolean> previousSnapshotValidatedMap = Maps.newHashMap();
    for (DeletedKeys bucketWithDeleteKeys : bucketDeletedKeysList) {
      // Validating previous snapshot since while purging rename keys, a snapshot create request could make this purge
      // rename entry invalid on AOS since the key could be now present on the newly created snapshot since a rename
      // request could have come through after creation of a snapshot. The purged deletedKey would not be even
      // present
      boolean isPreviousSnapshotValid = previousSnapshotValidatedMap.computeIfAbsent(
          Pair.of(bucketWithDeleteKeys.getVolumeName(), bucketWithDeleteKeys.getBucketName()),
          (volumeBucketPair) -> validatePreviousSnapshotId(fromSnapshotInfo, volumeBucketPair.getLeft(),
              volumeBucketPair.getRight(), omMetadataManager.getSnapshotChainManager(),
              bucketWithDeleteKeys.hasExpectedPathPreviousSnapshotUUID()
                  ? fromProtobuf(bucketWithDeleteKeys.getExpectedPathPreviousSnapshotUUID()) : null));
      if (isPreviousSnapshotValid) {
        renamedKeysToBePurged.addAll(bucketWithDeleteKeys.getRenamedKeysList());
        keysToBePurgedList.addAll(bucketWithDeleteKeys.getKeysList());
      }
    }

    if (keysToBePurgedList.isEmpty() && renamedKeysToBePurged.isEmpty()) {
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse,
          new OMException("None of the keys can be purged be purged since a new snapshot was created for all the " +
              "buckets, making this request invalid", OMException.ResultCodes.KEY_DELETION_ERROR)));
    }

    // Setting transaction info for snapshot, this is to prevent duplicate purge requests to OM from background
    // services.
    try {
      if (fromSnapshotInfo != null) {
        SnapshotUtils.setTransactionInfoInSnapshot(fromSnapshotInfo, termIndex);
        omMetadataManager.getSnapshotInfoTable().addCacheEntry(new CacheKey<>(fromSnapshotInfo.getTableKey()),
            CacheValue.get(termIndex.getIndex(), fromSnapshotInfo));
      }
    } catch (IOException e) {
      return new OMKeyPurgeResponse(createErrorOMResponse(omResponse, e));
    }

    return new OMKeyPurgeResponse(omResponse.build(),
        keysToBePurgedList, renamedKeysToBePurged, fromSnapshotInfo, keysToUpdateList);
  }

}
